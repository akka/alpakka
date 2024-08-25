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
import akka.http.scaladsl.model.headers.{
  ByteRange,
  CustomHeader,
  RawHeader,
  `Content-Length`,
  `Content-Type`,
  Range => RangeHeader
}
import akka.http.scaladsl.model.{
  ContentType,
  HttpEntity,
  HttpHeader,
  HttpMethod,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  ResponseEntity,
  StatusCode,
  Uri
}
import akka.http.scaladsl.unmarshalling.Unmarshal
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
                                 range: Option[ByteRange],
                                 versionId: Option[String],
                                 leaseId: Option[String]): Source[ByteString, Future[ObjectMetadata]] = {
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
            headers = populateCommonHeaders(HttpEntity.Empty, range = range, leaseId = leaseId)
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
  }

  private[storage] def getObjectProperties(storageType: String,
                                           objectPath: String,
                                           versionId: Option[String],
                                           leaseId: Option[String]): Source[Option[ObjectMetadata], NotUsed] = {
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
            headers = populateCommonHeaders(HttpEntity.Empty, leaseId = leaseId)
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
  }

  private[storage] def deleteObject(storageType: String,
                                    objectPath: String,
                                    versionId: Option[String],
                                    leaseId: Option[String]): Source[Option[ObjectMetadata], NotUsed] = {
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
            headers = populateCommonHeaders(HttpEntity.Empty, leaseId = leaseId)
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
  }

  private[storage] def putBlob(blobType: String,
                               objectPath: String,
                               contentType: ContentType,
                               contentLength: Long,
                               payload: Source[ByteString, _],
                               leaseId: Option[String]): Source[Option[ObjectMetadata], NotUsed] = {
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val httpEntity = HttpEntity(contentType, contentLength, payload)
        val request =
          createRequest(
            method = HttpMethods.PUT,
            uri = createUri(settings = settings,
                            storageType = BlobType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings)),
            headers = populateCommonHeaders(httpEntity, blobType = Some(blobType), leaseId = leaseId)
          ).withEntity(httpEntity)
        handlePutRequest(request, settings)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private[storage] def createFile(objectPath: String,
                                  contentType: ContentType,
                                  maxSize: Long,
                                  leaseId: Option[String]): Source[Option[ObjectMetadata], NotUsed] = {
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val request =
          createRequest(
            method = HttpMethods.PUT,
            uri = createUri(settings = settings,
                            storageType = FileType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings)),
            headers = Seq(
                CustomContentTypeHeader(contentType),
                RawHeader(XMsContentLengthHeaderKey, maxSize.toString),
                RawHeader(FileTypeHeaderKey, "file")
              ) ++ leaseId.map(value => RawHeader(LeaseIdHeaderKey, value))
          )
        handlePutRequest(request, settings)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private[storage] def updateOrClearRange(objectPath: String,
                                          contentType: ContentType,
                                          range: ByteRange.Slice,
                                          payload: Option[Source[ByteString, _]],
                                          leaseId: Option[String]): Source[Option[ObjectMetadata], NotUsed.type] = {
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val contentLength = range.last - range.first + 1
        val clearRange = payload.isEmpty
        val writeType = if (clearRange) "clear" else "update"
        val overrideContentLength = if (clearRange) Some(0L) else None
        val httpEntity =
          if (clearRange) HttpEntity.empty(contentType) else HttpEntity(contentType, contentLength, payload.get)
        val request =
          createRequest(
            method = HttpMethods.PUT,
            uri = createUri(settings = settings,
                            storageType = FileType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, Some("comp=range"))),
            headers = populateCommonHeaders(httpEntity,
                                            overrideContentLength,
                                            range = Some(range),
                                            leaseId = leaseId,
                                            writeType = Some(writeType))
          ).withEntity(httpEntity)
        handlePutRequest(request, settings)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private[storage] def createContainer(objectPath: String): Source[Option[ObjectMetadata], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val request =
          createRequest(
            method = HttpMethods.PUT,
            uri = createUri(settings = settings,
                            storageType = BlobType,
                            objectPath = objectPath,
                            queryString = createQueryString(settings, Some("restype=container"))),
            headers = populateCommonHeaders(HttpEntity.Empty)
          )

        handlePutRequest(request, settings)
      }
      .mapMaterializedValue(_ => NotUsed)

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
      .flatMapConcat(req => Signer(req, settings).signedRequest)
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

  private def populateCommonHeaders(entity: HttpEntity,
                                    overrideContentLength: Option[Long] = None,
                                    range: Option[ByteRange] = None,
                                    blobType: Option[String] = None,
                                    leaseId: Option[String] = None,
                                    writeType: Option[String] = None) = {
    // Azure required to have these two headers (Content-Length & Content-Type) in the request
    // in some cases Content-Length header must be set as 0
    val contentLength = overrideContentLength.orElse(entity.contentLengthOption).getOrElse(0L)
    val maybeContentLengthHeader =
      if (overrideContentLength.isEmpty && contentLength == 0L) None else Some(CustomContentLengthHeader(contentLength))

    val maybeContentTypeHeader =
      emptyStringToOption(entity.contentType.toString()) match {
        case Some(value) if value != "none/none" => Some(CustomContentTypeHeader(entity.contentType))
        case _ => None
      }

    val maybeRangeHeader = range.map(RangeHeader(_))
    val maybeBlobTypeHeader = blobType.map(value => RawHeader(BlobTypeHeaderKey, value))
    val maybeLeaseIdHeader = leaseId.map(value => RawHeader(LeaseIdHeaderKey, value))
    val maybeWriteTypeHeader = writeType.map(value => RawHeader(FileWriteTypeHeaderKey, value))

    (maybeContentLengthHeader ++ maybeContentTypeHeader ++ maybeRangeHeader ++ maybeBlobTypeHeader ++ maybeLeaseIdHeader ++ maybeWriteTypeHeader).toSeq
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

  private def createQueryString(settings: StorageSettings, apiQueryString: Option[String] = None) = {
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

  // `Content-Type` header is by design not accessible as header. So need to have a custom
  // header implementation to expose that
  private case class CustomContentTypeHeader(contentType: ContentType) extends CustomHeader {
    override def name(): String = "Content-Type"

    override def value(): String = contentType.value

    override def renderInRequests(): Boolean = true

    override def renderInResponses(): Boolean = true
  }

  private case class CustomContentLengthHeader(contentLength: Long) extends CustomHeader {
    override def name(): String = "Content-Length"

    override def value(): String = contentLength.toString

    override def renderInRequests(): Boolean = true

    override def renderInResponses(): Boolean = true
  }
}
