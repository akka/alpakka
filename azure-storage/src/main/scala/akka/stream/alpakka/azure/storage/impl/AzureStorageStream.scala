/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{Accepted, Created, NotFound, OK}
import akka.http.scaladsl.model.headers.{`Content-Length`, `Content-Type`}
import akka.http.scaladsl.model.{
  HttpEntity,
  HttpHeader,
  HttpMethod,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  ResponseEntity,
  StatusCode,
  UniversalEntity,
  Uri
}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.azure.storage.headers.CustomContentTypeHeader
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.azure.storage.impl.auth.Signer
import akka.stream.scaladsl.{Flow, RetryFlow, Source}
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object AzureStorageStream {

  private[storage] def getObject(storageType: String,
                                 objectPath: String,
                                 versionId: Option[String],
                                 headers: Seq[HttpHeader]): Source[ByteString, Future[ObjectMetadata]] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val request =
          createRequest(
            method = HttpMethods.GET,
            uri = createUri(settings = settings,
                            storageType = storageType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, versionId.map(value => s"versionId=$value"))),
            headers = headers
          )
        val objectMetadataMat = Promise[ObjectMetadata]()
        signAndRequest(request, settings)(mat.system)
          .map(response => response.withEntity(response.entity.withoutSizeLimit))
          .mapAsync(parallelism = 1)(entityForSuccess)
          .flatMapConcat {
            case (entity, headers) =>
              objectMetadataMat.success(computeMetaData(headers, entity))
              entity.dataBytes
          }
          .mapError {
            case e: Throwable =>
              objectMetadataMat.tryFailure(e)
              e
          }
          .mapMaterializedValue(_ => objectMetadataMat.future)
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.parasitic))

  private[storage] def getObjectProperties(storageType: String,
                                           objectPath: String,
                                           versionId: Option[String],
                                           headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        import mat.executionContext
        val settings = resolveSettings(attr, system)
        val request =
          createRequest(
            method = HttpMethods.HEAD,
            uri = createUri(settings = settings,
                            storageType = storageType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, versionId.map(value => s"versionId=$value"))),
            headers = headers
          )

        signAndRequest(request, settings)
          .flatMapConcat {
            case HttpResponse(OK, headers, entity, _) =>
              Source.future(
                entity.withoutSizeLimit().discardBytes().future().map(_ => Some(computeMetaData(headers, entity)))
              )
            case HttpResponse(NotFound, _, entity, _) =>
              Source.future(entity.discardBytes().future().map(_ => None)(ExecutionContexts.parasitic))
            case response: HttpResponse => Source.future(unmarshalError(response.status, response.entity))
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  private[storage] def deleteObject(storageType: String,
                                    objectPath: String,
                                    versionId: Option[String],
                                    headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        import mat.executionContext
        val settings = resolveSettings(attr, system)
        val request =
          createRequest(
            method = HttpMethods.DELETE,
            uri = createUri(settings = settings,
                            storageType = storageType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, versionId.map(value => s"versionId=$value"))),
            headers = headers
          )

        signAndRequest(request, settings)
          .flatMapConcat {
            case HttpResponse(Accepted, headers, entity, _) =>
              Source.future(
                entity.withoutSizeLimit().discardBytes().future().map(_ => Some(computeMetaData(headers, entity)))
              )
            case HttpResponse(NotFound, _, entity, _) =>
              Source.future(entity.discardBytes().future().map(_ => None)(ExecutionContexts.parasitic))
            case response: HttpResponse => Source.future(unmarshalError(response.status, response.entity))
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  private[storage] def putBlob(objectPath: String,
                               maybeHttpEntity: Option[UniversalEntity],
                               headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    putRequest(objectPath = objectPath, storageType = BlobType, maybeHttpEntity = maybeHttpEntity, headers = headers)

  private[storage] def createFile(objectPath: String,
                                  headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    putRequest(objectPath = objectPath, storageType = FileType, headers = headers)

  private[storage] def updateRange(objectPath: String,
                                   httpEntity: UniversalEntity,
                                   headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    putRequest(objectPath = objectPath,
               storageType = FileType,
               maybeHttpEntity = Some(httpEntity),
               queryString = Some("comp=range"),
               headers = headers)

  private[storage] def clearRange(objectPath: String,
                                  headers: Seq[HttpHeader]): Source[Option[ObjectMetadata], NotUsed] =
    putRequest(objectPath = objectPath, storageType = FileType, queryString = Some("comp=range"), headers = headers)

  private[storage] def createContainer(objectPath: String): Source[Option[ObjectMetadata], NotUsed] =
    putRequest(objectPath = objectPath, storageType = BlobType, queryString = Some("restype=container"))

  /**
   * Common function for "PUT" request where we don't expect response body.
   *
   * @param objectPath path of the object.
   * @param storageType storage type
   * @param maybeHttpEntity optional http entity
   * @param queryString query string
   * @param headers request headers
   * @return Source with metadata containing response headers
   */
  private def putRequest(objectPath: String,
                         storageType: String,
                         maybeHttpEntity: Option[UniversalEntity] = None,
                         queryString: Option[String] = None,
                         headers: Seq[HttpHeader] = Seq.empty): Source[Option[ObjectMetadata], NotUsed] = {
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val httpEntity = maybeHttpEntity.getOrElse(HttpEntity.Empty)
        val request =
          createRequest(
            method = HttpMethods.PUT,
            uri = createUri(settings = settings,
                            storageType = storageType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, queryString)),
            headers = headers
          ).withEntity(httpEntity)
        handlePutRequest(request, settings)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def handlePutRequest(request: HttpRequest, settings: StorageSettings)(implicit system: ActorSystem) = {
    import system.dispatcher
    signAndRequest(request, settings).flatMapConcat {
      case HttpResponse(Created, h, entity, _) =>
        Source.future(entity.discardBytes().future().map { _ =>
          val contentLengthHeader = `Content-Length`
            .parseFromValueString(entity.contentLengthOption.getOrElse(0L).toString)
            .map(Seq(_))
            .getOrElse(Nil)
          Some(ObjectMetadata(h ++ contentLengthHeader))
        })
      case HttpResponse(NotFound, _, entity, _) =>
        Source.future(entity.discardBytes().future().map(_ => None)(ExecutionContexts.parasitic))
      case response: HttpResponse => Source.future(unmarshalError(response.status, response.entity))
    }
  }

  private def signAndRequest(
      request: HttpRequest,
      settings: StorageSettings
  )(implicit
    system: ActorSystem): Source[HttpResponse, NotUsed] = {
    import system.dispatcher

    val retriableFlow = Flow[HttpRequest]
      .mapAsync(parallelism = 1)(
        req =>
          singleRequest(req)
            .map(Success.apply)
            .recover[Try[HttpResponse]] {
              case t =>
                Failure(t)
            }
      )

    import settings.retrySettings._
    Source
      .single(request)
      .flatMapConcat(request => Signer(request, settings).signedRequest)
      .via(RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, retriableFlow) {
        case (request, Success(response)) if isTransientError(response.status) =>
          response.entity.discardBytes()
          Some(request)
        case (request, Failure(_)) =>
          // Treat any exception as transient.
          Some(request)
        case _ => None
      })
      .mapAsync(1)(Future.fromTry)
  }

  private def singleRequest(request: HttpRequest)(implicit system: ActorSystem) =
    Http().singleRequest(request)

  private def computeMetaData(headers: immutable.Seq[HttpHeader], entity: ResponseEntity): ObjectMetadata = {
    val contentLengthHeader: Seq[HttpHeader] = `Content-Length`
      .parseFromValueString(entity.contentLengthOption.getOrElse(0L).toString)
      .map(Seq(_))
      .getOrElse(Nil)
    val contentTypeHeader: Seq[HttpHeader] = `Content-Type`
      .parseFromValueString(entity.contentType.value)
      .map(Seq(_))
      .getOrElse(Nil)
    ObjectMetadata(
      headers ++
      contentLengthHeader ++ contentTypeHeader ++
      immutable.Seq(
        CustomContentTypeHeader(entity.contentType)
      )
    )
  }

  private def isTransientError(status: StatusCode): Boolean = {
    // 5xx errors from S3 can be treated as transient.
    status.intValue >= 500
  }

  private def unmarshalError(code: StatusCode, entity: ResponseEntity)(implicit mat: Materializer) = {
    import mat.executionContext
    Unmarshal(entity).to[String].map { err =>
      throw StorageException(err, code)
    }
  }

  private def entityForSuccess(
      resp: HttpResponse
  )(implicit mat: Materializer): Future[(ResponseEntity, Seq[HttpHeader])] = {
    resp match {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful((entity, headers))
      case response: HttpResponse =>
        unmarshalError(response.status, response.entity)
    }
  }

  private def createRequest(method: HttpMethod, uri: Uri, headers: Seq[HttpHeader]) =
    HttpRequest(method = method, uri = uri, headers = headers)

  private def createUri(settings: StorageSettings,
                        storageType: String,
                        objectPath: String,
                        queryString: Option[String]) = {
    val accountName = settings.azureNameKeyCredential.accountName
    val path = if (objectPath.startsWith("/")) objectPath else s"/$objectPath"
    settings.endPointUrl
      .map { endPointUrl =>
        val qs = queryString.getOrElse("")
        Uri(endPointUrl).withPath(Uri.Path(s"/$accountName$path")).withQuery(Uri.Query(qs))
      }
      .getOrElse(
        Uri.from(
          scheme = "https",
          host = s"$accountName.$storageType.core.windows.net",
          path = Uri.Path(path).toString(),
          queryString = queryString
        )
      )
  }

  private def createQueryString(settings: StorageSettings, apiQueryString: Option[String]) = {
    if (settings.authorizationType == SasAuthorizationType) {
      if (settings.sasToken.isEmpty) throw new RuntimeException("SAS token must be defined for SAS authorization type.")
      else {
        val sasToken = settings.sasToken.get
        Some(apiQueryString.map(qs => s"$sasToken&$qs").getOrElse(sasToken))
      }
    } else apiQueryString
  }

  private def resolveSettings(attr: Attributes, sys: ActorSystem) =
    attr
      .get[StorageSettingsValue]
      .map(_.settings)
      .getOrElse {
        val storageExtension = StorageExt(sys)
        attr
          .get[StorageSettingsPath]
          .map(settingsPath => storageExtension.settings(settingsPath.path))
          .getOrElse(storageExtension.settings)
      }
}
