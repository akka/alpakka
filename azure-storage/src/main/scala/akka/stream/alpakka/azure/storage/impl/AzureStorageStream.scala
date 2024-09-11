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
  HttpRequest,
  HttpResponse,
  MessageEntity,
  ResponseEntity,
  StatusCode
}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.azure.storage.headers.CustomContentTypeHeader
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.azure.storage.impl.auth.Signer
import akka.stream.alpakka.azure.storage.requests.{
  ClearFileRange,
  CreateContainer,
  CreateDirectory,
  CreateFile,
  DeleteContainer,
  DeleteDirectory,
  GetProperties,
  RequestBuilder,
  UpdateFileRange
}
import akka.stream.scaladsl.{Flow, RetryFlow, Source}
import akka.util.ByteString

import java.time.Clock
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object AzureStorageStream {

  private[storage] def getObject(storageType: String,
                                 objectPath: String,
                                 requestBuilder: RequestBuilder): Source[ByteString, Future[ObjectMetadata]] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        val settings = resolveSettings(attr, system)
        val request = requestBuilder.createRequest(settings, storageType, objectPath)
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

  private[storage] def getBlobProperties(objectPath: String,
                                         requestBuilder: GetProperties): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = OK, storageType = BlobType, objectPath = objectPath, requestBuilder = requestBuilder)

  private[storage] def getFileProperties(objectPath: String,
                                         requestBuilder: GetProperties): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = OK, storageType = FileType, objectPath = objectPath, requestBuilder = requestBuilder)

  private[storage] def deleteBlob(objectPath: String,
                                  requestBuilder: RequestBuilder): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Accepted,
                  storageType = BlobType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def deleteFile(objectPath: String,
                                  requestBuilder: RequestBuilder): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Accepted,
                  storageType = FileType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def putBlob(objectPath: String,
                               requestBuilder: RequestBuilder,
                               maybeHttpEntity: Option[MessageEntity]): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Created,
                  storageType = BlobType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder,
                  maybeHttpEntity = maybeHttpEntity)

  private[storage] def createFile(objectPath: String,
                                  requestBuilder: CreateFile): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Created,
                  storageType = FileType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def updateRange(objectPath: String,
                                   httpEntity: MessageEntity,
                                   requestBuilder: UpdateFileRange): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(
      successCode = Created,
      storageType = FileType,
      objectPath = objectPath,
      requestBuilder = requestBuilder,
      maybeHttpEntity = Some(httpEntity)
    )

  private[storage] def clearRange(objectPath: String,
                                  requestBuilder: ClearFileRange): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Created,
                  storageType = FileType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def createContainer(objectPath: String,
                                       requestBuilder: CreateContainer): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Created,
                  storageType = BlobType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def deleteContainer(objectPath: String,
                                       requestBuilder: DeleteContainer): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Accepted,
                  storageType = BlobType,
                  objectPath = objectPath,
                  requestBuilder = requestBuilder)

  private[storage] def createDirectory(directoryPath: String,
                                       requestBuilder: CreateDirectory): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Created,
                  storageType = FileType,
                  objectPath = directoryPath,
                  requestBuilder = requestBuilder)

  private[storage] def deleteDirectory(directoryPath: String,
                                       requestBuilder: DeleteDirectory): Source[Option[ObjectMetadata], NotUsed] =
    handleRequest(successCode = Accepted,
                  storageType = FileType,
                  objectPath = directoryPath,
                  requestBuilder = requestBuilder)

  /**
   * Common function to handle all requests where we don't expect response body.
   *
   * @param successCode status code for successful response
   * @param storageType storage type
   * @param objectPath path of the object.
   * @param requestBuilder request builder
   * @param maybeHttpEntity optional http entity
   * @return Source with metadata containing response headers
   */
  private def handleRequest(
      successCode: StatusCode,
      storageType: String,
      objectPath: String,
      requestBuilder: RequestBuilder,
      maybeHttpEntity: Option[MessageEntity] = None
  ): Source[Option[ObjectMetadata], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        import system.dispatcher
        val settings = resolveSettings(attr, system)
        val httpEntity = maybeHttpEntity.getOrElse(HttpEntity.Empty)
        val request = requestBuilder.createRequest(settings, storageType, objectPath).withEntity(httpEntity)
        signAndRequest(request, settings).flatMapConcat {
          case HttpResponse(sc, h, entity, _) if sc == successCode =>
            Source.future(entity.withoutSizeLimit().discardBytes().future().map(_ => Some(computeMetaData(h, entity))))
          case HttpResponse(NotFound, _, entity, _) =>
            Source.future(
              entity
                .withoutSizeLimit()
                .discardBytes()
                .future()
                .map(_ => None)(ExecutionContexts.parasitic)
            )
          case response: HttpResponse => Source.future(unmarshalError(response.status, response.entity))
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def signAndRequest(
      request: HttpRequest,
      settings: StorageSettings
  )(implicit
    system: ActorSystem): Source[HttpResponse, NotUsed] = {
    import system.dispatcher

    val retriableFlow = Flow[HttpRequest]
      .mapAsync(parallelism = 1)(
        req =>
          Http()
            .singleRequest(req)
            .map(Success.apply)
            .recover[Try[HttpResponse]] {
              case t =>
                Failure(t)
            }
      )

    import settings.retrySettings._
    implicit val clock: Clock = Clock.systemUTC()
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

  private def computeMetaData(headers: Seq[HttpHeader], entity: ResponseEntity): ObjectMetadata = {
    val contentLengthHeader = `Content-Length`
      .parseFromValueString(entity.contentLengthOption.getOrElse(0L).toString)
      .map(Seq(_))
      .getOrElse(Nil)
    val contentTypeHeader = `Content-Type`
      .parseFromValueString(entity.contentType.value)
      .map(Seq(_))
      .getOrElse(Nil)
    ObjectMetadata(
      headers ++ contentLengthHeader ++ contentTypeHeader ++ Seq(CustomContentTypeHeader(entity.contentType))
    )
  }

  private def isTransientError(status: StatusCode): Boolean = {
    // 5xx errors from Azure Blob Storage can be treated as transient.
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
