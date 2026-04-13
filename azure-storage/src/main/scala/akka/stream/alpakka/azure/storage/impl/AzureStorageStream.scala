/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{Accepted, Created, NotFound, OK}
import akka.http.scaladsl.model.headers.{`Content-Length`, `Content-Type`}
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpHeader,
  HttpRequest,
  HttpResponse,
  MessageEntity,
  ResponseEntity,
  StatusCode
}
import akka.http.scaladsl.unmarshalling.Unmarshal
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
  ListBlobs,
  ListFiles,
  PutBlock,
  PutBlockBlobStreaming,
  PutBlockList,
  RequestBuilder,
  UpdateFileRange
}
import akka.stream.scaladsl.{Flow, Keep, RetryFlow, Sink, Source}
import akka.util.ByteString

import java.nio.charset.StandardCharsets
import java.time.Clock
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.xml.XML

object AzureStorageStream {

  private val futureNone = Future.successful(None)

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
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContext.parasitic))

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

  /**
   * Uploads a block blob using streaming Put Block / Put Block List operations.
   * The incoming bytes are grouped into blocks of the configured size, each uploaded individually,
   * then committed as a single blob. No need to know the total content length upfront.
   */
  private[storage] def putBlockBlobStreaming(
      objectPath: String,
      requestBuilder: PutBlockBlobStreaming
  ): Sink[ByteString, Future[ObjectMetadata]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        import system.dispatcher
        val settings = resolveSettings(attr, system)

        Flow[ByteString]
          .via(rechunkFlow(requestBuilder.blockSize))
          .statefulMap(() => 0)(
            (index, block) => {
              val blockId = encodeBlockId(index)
              (index + 1, (blockId, block))
            },
            _ => None
          )
          .mapAsync(1) {
            case (blockId, block) =>
              val putBlock = new PutBlock(blockId, block.length.toLong, requestBuilder.contentType,
                                          leaseId = requestBuilder.leaseId,
                                          sse = requestBuilder.sse,
                                          additionalHeaders = requestBuilder.additionalHeaders)
              val entity = HttpEntity.Strict(requestBuilder.contentType, block)
              val request = putBlock.createRequest(settings, BlobType, objectPath).withEntity(entity)
              signAndRequest(request, settings)
                .flatMapConcat {
                  case HttpResponse(Created, _, responseEntity, _) =>
                    Source.future(responseEntity.discardBytes().future().map(_ => blockId))
                  case response: HttpResponse =>
                    Source.future(unmarshalError(response.status, response.entity))
                }
                .runWith(Sink.head)
          }
          .fold(Seq.empty[String])(_ :+ _)
          .mapAsync(1) { blockIds =>
            val blockListXml = buildBlockListXml(blockIds)
            val xmlBytes = ByteString(blockListXml)
            val putBlockList = new PutBlockList(xmlBytes.length.toLong,
                                                leaseId = requestBuilder.leaseId,
                                                sse = requestBuilder.sse,
                                                additionalHeaders = requestBuilder.additionalHeaders)
            val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, xmlBytes)
            val request = putBlockList.createRequest(settings, BlobType, objectPath).withEntity(entity)
            signAndRequest(request, settings)
              .flatMapConcat {
                case HttpResponse(Created, h, responseEntity, _) =>
                  Source.future(
                    responseEntity.withoutSizeLimit().discardBytes().future().map(_ => computeMetaData(h, responseEntity))
                  )
                case response: HttpResponse =>
                  Source.future(unmarshalError(response.status, response.entity))
              }
              .runWith(Sink.head)
          }
          .toMat(Sink.head)(Keep.right)
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContext.parasitic))

  private def rechunkFlow(blockSize: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .statefulMap(() => ByteString.empty)(
        (buffer, elem) => {
          val combined = buffer ++ elem
          if (combined.length >= blockSize) {
            val blocks = combined.grouped(blockSize).toList
            // Last chunk may be smaller than blockSize, keep it as buffer
            if (combined.length % blockSize == 0)
              (ByteString.empty, blocks)
            else
              (blocks.last, blocks.init)
          } else {
            (combined, Nil)
          }
        },
        buffer => if (buffer.nonEmpty) Some(List(buffer)) else Some(Nil)
      )
      .mapConcat(identity)

  private def encodeBlockId(index: Int): String =
    Base64.getEncoder.encodeToString(f"$index%06d".getBytes(StandardCharsets.UTF_8))

  private def buildBlockListXml(blockIds: Seq[String]): String = {
    val entries = blockIds.map(id => s"  <Latest>$id</Latest>").mkString("\n")
    s"""<?xml version="1.0" encoding="utf-8"?>
       |<BlockList>
       |$entries
       |</BlockList>""".stripMargin
  }

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
   * Lists blobs in a container, automatically following pagination markers.
   *
   * @param objectPath container name, e.g. `my-container`
   * @param requestBuilder builder to configure the list request
   * @return Source of [[BlobItem]] elements
   */
  private[storage] def listBlobs(objectPath: String, requestBuilder: ListBlobs): Source[BlobItem, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        implicit val materializer: Materializer = mat
        import system.dispatcher
        val settings = resolveSettings(attr, system)

        Source
          .unfoldAsync[Option[String], Seq[BlobItem]](Some("")) {
            case None => futureNone
            case Some(marker) =>
              val rb = if (marker.isEmpty) requestBuilder else requestBuilder.withMarker(marker)
              signAndRequest(rb.createRequest(settings, BlobType, objectPath), settings)
                .flatMapConcat {
                  case HttpResponse(OK, _, entity, _) =>
                    Source.future(
                      Unmarshal(entity).to[String].map { xml =>
                        val result = parseBlobListXml(xml)
                        Some((result.nextMarker, result.items))
                      }
                    )
                  case response: HttpResponse =>
                    Source.future(unmarshalError(response.status, response.entity))
                }
                .runWith(Sink.head)
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Lists files and directories in an Azure File Share directory, automatically following pagination markers.
   *
   * @param objectPath share and directory path, e.g. `my-share` or `my-share/my-directory`
   * @param requestBuilder builder to configure the list request
   * @return Source of [[FileShareEntry]] elements (either [[ShareFileItem]] or [[ShareDirectoryItem]])
   */
  private[storage] def listFiles(objectPath: String, requestBuilder: ListFiles): Source[FileShareEntry, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        implicit val materializer: Materializer = mat
        import system.dispatcher
        val settings = resolveSettings(attr, system)

        Source
          .unfoldAsync[Option[String], Seq[FileShareEntry]](Some("")) {
            case None => futureNone
            case Some(marker) =>
              val rb = if (marker.isEmpty) requestBuilder else requestBuilder.withMarker(marker)
              signAndRequest(rb.createRequest(settings, FileType, objectPath), settings)
                .flatMapConcat {
                  case HttpResponse(OK, _, entity, _) =>
                    Source.future(
                      Unmarshal(entity).to[String].map { xml =>
                        val result = parseFileListXml(xml)
                        Some((result.nextMarker, result.entries))
                      }
                    )
                  case response: HttpResponse =>
                    Source.future(unmarshalError(response.status, response.entity))
                }
                .runWith(Sink.head)
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

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
                .map(_ => None)(ExecutionContext.parasitic)
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
    ObjectMetadata(headers ++ contentLengthHeader ++ contentTypeHeader)
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

  private case class BlobListResult(items: Seq[BlobItem], nextMarker: Option[String])
  private case class FileListResult(entries: Seq[FileShareEntry], nextMarker: Option[String])

  // Azure Storage list responses include a UTF-8 BOM at the start of the body. After the entity
  // is decoded to String the BOM survives as a literal U+FEFF character, which scala.xml's
  // loadString rejects with "Content is not allowed in prolog". Strip it before parsing.
  private def loadAzureXml(rawXml: String): scala.xml.Elem = {
    val stripped = if (rawXml.nonEmpty && rawXml.charAt(0) == '\uFEFF') rawXml.substring(1) else rawXml
    XML.loadString(stripped)
  }

  private def parseBlobListXml(rawXml: String): BlobListResult = {
    val root = loadAzureXml(rawXml)
    val items = (root \\ "Blob").map { blob =>
      val name = (blob \ "Name").text
      val props = blob \ "Properties"
      val eTag = emptyStringToOption((props \ "Etag").text).map(removeQuotes)
      val contentLength = (props \ "Content-Length").text.toLongOption.getOrElse(0L)
      val contentType = emptyStringToOption((props \ "Content-Type").text)
      val lastModified = emptyStringToOption((props \ "Last-Modified").text)
      val blobType = (props \ "BlobType").text
      BlobItem(name, eTag, contentLength, contentType, lastModified, blobType)
    }
    val nextMarker = emptyStringToOption((root \ "NextMarker").text)
    BlobListResult(items, nextMarker)
  }

  private def parseFileListXml(rawXml: String): FileListResult = {
    val root = loadAzureXml(rawXml)
    val entries = (root \\ "Entries").flatMap(_.child).collect {
      case file if file.label == "File" =>
        val name = (file \ "Name").text
        val contentLength = (file \ "Properties" \ "Content-Length").text.toLongOption.getOrElse(0L)
        ShareFileItem(name, contentLength): FileShareEntry
      case dir if dir.label == "Directory" =>
        ShareDirectoryItem((dir \ "Name").text): FileShareEntry
    }
    val nextMarker = emptyStringToOption((root \ "NextMarker").text)
    FileListResult(entries, nextMarker)
  }
}
