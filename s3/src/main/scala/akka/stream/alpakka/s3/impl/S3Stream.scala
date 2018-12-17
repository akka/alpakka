/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.time.{Instant, LocalDate}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success}
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{NoContent, NotFound, OK}
import akka.http.scaladsl.model.headers.{ByteRange, CustomHeader, `Content-Length`}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.s3.auth.{CredentialScope, Signer, SigningKey}
import akka.stream.alpakka.s3.scaladsl.{ListBucketResultContents, ObjectMetadata}
import akka.stream.alpakka.s3._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

final case class S3Location(bucket: String, key: String)

final case class MultipartUpload(s3Location: S3Location, uploadId: String)

sealed trait UploadPartResponse {
  def multipartUpload: MultipartUpload

  def index: Int
}

final case class SuccessfulUploadPart(multipartUpload: MultipartUpload, index: Int, etag: String)
    extends UploadPartResponse

final case class FailedUploadPart(multipartUpload: MultipartUpload, index: Int, exception: Throwable)
    extends UploadPartResponse

final case class FailedUpload(reasons: Seq[Throwable]) extends Exception(reasons.map(_.getMessage).mkString(", "))

final case class CompleteMultipartUploadResult(location: Uri,
                                               bucket: String,
                                               key: String,
                                               etag: String,
                                               versionId: Option[String] = None)

final case class ListBucketResult(isTruncated: Boolean,
                                  continuationToken: Option[String],
                                  contents: Seq[ListBucketResultContents])

sealed trait ApiVersion {
  def getInstance: ApiVersion
}

case object ListBucketVersion1 extends ApiVersion {
  override val getInstance: ApiVersion = ListBucketVersion1
}
case object ListBucketVersion2 extends ApiVersion {
  override val getInstance: ApiVersion = ListBucketVersion2
}

final case class CopyPartResult(lastModified: Instant, eTag: String)

final case class CopyPartition(partNumber: Int, sourceLocation: S3Location, range: Option[ByteRange.Slice] = None)

final case class MultipartCopy(multipartUpload: MultipartUpload, copyPartition: CopyPartition)

private[alpakka] object S3Stream {

  import HttpRequests._
  import Marshalling._

  val MinChunkSize = 5242880 //in bytes
  // def because tokens can expire
  def signingKey(implicit settings: S3Settings) = SigningKey(
    settings.credentialsProvider,
    CredentialScope(LocalDate.now(), settings.s3RegionProvider.getRegion, "s3")
  )

  def download(s3Location: S3Location,
               range: Option[ByteRange],
               versionId: Option[String],
               sse: Option[ServerSideEncryption]): Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] = {
    val s3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(GetObject) })

    MaterializerAccess.source { implicit mat =>
      request(s3Location, rangeOption = range, versionId = versionId, s3Headers = s3Headers)
        .map(response => response.withEntity(response.entity.withoutSizeLimit))
        .mapAsync(parallelism = 1)(entityForSuccess)
        .map {
          case (entity, headers) =>
            Some((entity.dataBytes.mapMaterializedValue(_ => NotUsed), computeMetaData(headers, entity)))
        }
        .recover {
          case e: S3Exception if e.code == "NoSuchKey" => None[Option[(Source[ByteString, NotUsed], ObjectMetadata)]]
        }
      }
  }.mapMaterializedValue(_ => NotUsed)

  def listBucket(bucket: String, prefix: Option[String] = None): Source[ListBucketResultContents, NotUsed] = {
    sealed trait ListBucketState
    case object Starting extends ListBucketState
    case class Running(continuationToken: String) extends ListBucketState
    case object Finished extends ListBucketState

    def listBucketCall(
        token: Option[String]
    )(implicit mat: ActorMaterializer): Future[Option[(ListBucketState, Seq[ListBucketResultContents])]] = {
      import mat.executionContext

      implicit val conf = S3Ext(mat.system).settings

      signAndGetAs[ListBucketResult](HttpRequests.listBucket(bucket, prefix, token))
        .map { (res: ListBucketResult) =>
          Some(
            res.continuationToken
              .fold[(ListBucketState, Seq[ListBucketResultContents])]((Finished, res.contents))(
                t => (Running(t), res.contents)
              )
          )
        }
      }

    MaterializerAccess.source { implicit mat =>
      Source
        .unfoldAsync[ListBucketState, Seq[ListBucketResultContents]](Starting) {
          case Finished => Future.successful(None)
          case Starting => listBucketCall(None)
          case Running(token) => listBucketCall(Some(token))
        }
        .mapConcat(identity)
    }.mapMaterializedValue(_ => NotUsed)
  }

  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Option[String],
                        sse: Option[ServerSideEncryption]): Source[Option[ObjectMetadata], NotUsed] = {
    MaterializerAccess.source { implicit mat =>
      import mat.executionContext
      val s3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(HeadObject) })
      request(S3Location(bucket, key), HttpMethods.HEAD, versionId = versionId, s3Headers = s3Headers).flatMapConcat {
        case HttpResponse(OK, headers, entity, _) =>
          Source.fromFuture {
            entity.withoutSizeLimit().discardBytes().future().map { _ =>
              Some(computeMetaData(headers, entity))
            }
          }
        case HttpResponse(NotFound, _, entity, _) =>
          Source.fromFuture(entity.discardBytes().future().map(_ => None))
        case HttpResponse(_, _, entity, _) =>
          Source.fromFuture {
            Unmarshal(entity).to[String].map { err =>
              throw new S3Exception(err)
            }
          }
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  def deleteObject(s3Location: S3Location, versionId: Option[String]): Source[Done, NotUsed] = {
    MaterializerAccess.source { implicit mat =>
      import mat.executionContext
      request(s3Location, HttpMethods.DELETE, versionId = versionId).flatMapConcat {
        case HttpResponse(NoContent, _, entity, _) =>
          Source.fromFuture(entity.discardBytes().future().map(_ => Done))
        case HttpResponse(_, _, entity, _) =>
          Source.fromFuture {
            Unmarshal(entity).to[String].map { err =>
              throw new S3Exception(err)
            }
          }
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  def putObject(s3Location: S3Location,
                contentType: ContentType,
                data: Source[ByteString, _],
                contentLength: Long,
                s3Headers: S3Headers,
                sse: Option[ServerSideEncryption]): Source[ObjectMetadata, NotUsed] = {

    // TODO can we take in a Source[ByteString, NotUsed] without forcing chunking
    // chunked requests are causing S3 to think this is a multipart upload

    val headers = S3Headers(
      s3Headers.headers ++ sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(PutObject) }
    )

    MaterializerAccess.source { implicit mat =>
      import mat.executionContext
      implicit val conf = S3Ext(mat.system).settings

      val req = uploadRequest(s3Location, data, contentLength, contentType, headers)

      signAndRequest(req)
        .flatMapConcat {
        case HttpResponse(OK, h, entity, _) =>
          Source.fromFuture {
            entity.discardBytes().future().map { _ =>
              ObjectMetadata(h :+ `Content-Length`(entity.contentLengthOption.getOrElse(0)))
            }
          }
        case HttpResponse(_, _, entity, _) =>
          Source.fromFuture {
            Unmarshal(entity).to[String].map { err =>
              throw new S3Exception(err)
            }
          }
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  def request(s3Location: S3Location,
              method: HttpMethod = HttpMethods.GET,
              rangeOption: Option[ByteRange] = None,
              versionId: Option[String] = None,
              s3Headers: S3Headers = S3Headers.empty): Source[HttpResponse, NotUsed] =
    MaterializerAccess.source { mat =>
      implicit val conf = S3Ext(mat.system).settings
      signAndRequest(requestHeaders(getDownloadRequest(s3Location, method, s3Headers, versionId), rangeOption))
    }.mapMaterializedValue(_ => NotUsed)

  private def requestHeaders(downloadRequest: HttpRequest, rangeOption: Option[ByteRange]): HttpRequest =
    rangeOption match {
      case Some(range) => downloadRequest.addHeader(headers.Range(range))
      case _ => downloadRequest
    }

  /**
   * Uploads a stream of ByteStrings to a specified location as a multipart upload.
   */
  def multipartUpload(
      s3Location: S3Location,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      sse: Option[ServerSideEncryption] = None,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Sink[ByteString, Future[CompleteMultipartUploadResult]] =
    chunkAndRequest(s3Location, contentType, s3Headers, chunkSize, sse)(chunkingParallelism)
      .toMat(completionSink(s3Location))(Keep.right)

  private def initiateMultipartUpload(s3Location: S3Location,
                                      contentType: ContentType,
                                      s3Headers: S3Headers): Source[MultipartUpload, NotUsed] = {
    MaterializerAccess.source { implicit mat =>
      import mat.executionContext
      implicit val conf = S3Ext(mat.system).settings

      val req = initiateMultipartUploadRequest(s3Location, contentType, s3Headers)

      signAndRequest(req).flatMapConcat {
        case HttpResponse(status, _, entity, _) if status.isSuccess() =>
          Source.fromFuture(Unmarshal(entity).to[MultipartUpload])
        case HttpResponse(_, _, entity, _) =>
          Source.fromFuture {
            Unmarshal(entity).to[String].map { err =>
              throw new S3Exception(err)
            }
          }
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  def multipartCopy[T](
      sourceLocation: S3Location,
      targetLocation: S3Location,
      sourceVersionId: Option[String] = None,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      transformResult: CompleteMultipartUploadResult => T,
      sse: Option[ServerSideEncryption] = None,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): RunnableGraph[Future[T]] = {

    // Pre step get source meta to get content length (size of the object)
    val eventualMaybeObjectSize =
      getObjectMetadata(sourceLocation.bucket, sourceLocation.key, sourceVersionId, sse).map(_.map(_.contentLength))
    val eventualPartitions =
      eventualMaybeObjectSize.map(_.map(createPartitions(chunkSize, sourceLocation)).getOrElse(Nil))

    // Multipart copy upload requests (except for the completion api) are created here.
    //  The initial copy upload request gets executed within this function as well.
    //  The individual copy upload part requests are created.
    val copyRequests =
      createCopyRequests(targetLocation, sourceVersionId, contentType, s3Headers, sse, eventualPartitions)(chunkingParallelism)

    // The individual copy upload part requests are processed here
    processUploadCopyPartRequests(copyRequests)(chunkingParallelism)
      .toMat(completionSink(targetLocation, transformResult))(Keep.right)
  }

  private def computeMetaData(headers: Seq[HttpHeader], entity: ResponseEntity): ObjectMetadata =
    ObjectMetadata(
      headers ++
      Seq(
        `Content-Length`(entity.contentLengthOption.getOrElse(0)),
        CustomContentTypeHeader(entity.contentType)
      )
    )

  //`Content-Type` header is by design not accessible as header. So need to have a custom
  //header implementation to expose that
  private case class CustomContentTypeHeader(contentType: ContentType) extends CustomHeader {
    override def name(): String = "Content-Type"
    override def value(): String = contentType.value
    override def renderInRequests(): Boolean = true
    override def renderInResponses(): Boolean = true
  }

  private def completeMultipartUpload(s3Location: S3Location,
                                      parts: Seq[SuccessfulUploadPart])(implicit mat: ActorMaterializer): Future[CompleteMultipartUploadResult] = {
    def populateResult(result: CompleteMultipartUploadResult,
                       headers: Seq[HttpHeader]): CompleteMultipartUploadResult = {
      val versionId = headers.find(_.lowercaseName() == "x-amz-version-id").map(_.value())
      result.copy(versionId = versionId)
    }

    import mat.executionContext
    implicit val conf = S3Ext(mat.system).settings

    for {
      req <- completeMultipartUploadRequest(parts.head.multipartUpload, parts.map(p => p.index -> p.etag))
      result <- signAndGetAs[CompleteMultipartUploadResult](req, populateResult(_, _))
    } yield result
  }

  /**
   * Initiates a multipart upload. Returns a source of the initiated upload with upload part indicess
   */
  private def initiateUpload(s3Location: S3Location,
                             contentType: ContentType,
                             s3Headers: S3Headers): Source[(MultipartUpload, Int), NotUsed] =
    Source
      .single(s3Location)
      .flatMapConcat(initiateMultipartUpload(_, contentType, s3Headers))
      .mapConcat(r => Stream.continually(r))
      .zip(Source.fromIterator(() => Iterator.from(1)))

  val atLeastOneByteString = Flow[ByteString].orElse(Source.single(ByteString.empty))

  private def createRequests(
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int,
      parallelism: Int,
      sse: Option[ServerSideEncryption]
  ): Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] = {

    assert(
      chunkSize >= MinChunkSize,
      "Chunk size must be at least 5242880B. See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html"
    )

    // First step of the multi part upload process is made.
    //  The response is then used to construct the subsequent individual upload part requests
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] =
      initiateUpload(s3Location,
                     contentType,
                     S3Headers(
                       s3Headers.headers ++
                       sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(InitiateMultipartUpload) }
                     ))

    val headers: S3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(UploadPart) })

    MaterializerAccess.flow { mat =>
      implicit val conf = S3Ext(mat.system).settings

      SplitAfterSize(chunkSize)(atLeastOneByteString)
        .via(getChunkBuffer(chunkSize)) //creates the chunks
        .concatSubstreams
        .zipWith(requestInfo) {
          case (chunkedPayload, (uploadInfo, chunkIndex)) =>
            //each of the payload requests are created
            val partRequest =
              uploadPartRequest(uploadInfo, chunkIndex, chunkedPayload.data, chunkedPayload.size, headers)
            (partRequest, (uploadInfo, chunkIndex))
        }
        .flatMapConcat { case (req, info) => Signer.signedRequest(req, signingKey).zip(Source.single(info)) }
    }.mapMaterializedValue(_ => NotUsed)
  }

  private def getChunkBuffer(chunkSize: Int)(implicit settings: S3Settings) = settings.bufferType match {
    case MemoryBufferType =>
      new MemoryBuffer(chunkSize * 2)
    case d @ DiskBufferType(_) =>
      new DiskBuffer(2, chunkSize * 2, d.path)
  }

  private def chunkAndRequest(
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int,
      sse: Option[ServerSideEncryption]
  )(parallelism: Int): Flow[ByteString, UploadPartResponse, NotUsed] = {

    // Multipart upload requests (except for the completion api) are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.
    val requestFlow = createRequests(s3Location, contentType, s3Headers, chunkSize, parallelism, sse)

    // The individual upload part requests are processed here
    MaterializerAccess.flow { implicit mat =>
      import mat.executionContext
      implicit val sys = mat.system

      requestFlow
        .via(Http().superPool[(MultipartUpload, Int)]())
        .mapAsync(parallelism) {
          case (Success(r), (upload, index)) =>
            if (r.status.isFailure()) {
              Unmarshal(r.entity).to[String].map { errorBody =>
                FailedUploadPart(
                  upload,
                  index,
                  new RuntimeException(
                    s"Upload part $index request failed. Response header: ($r), response body: ($errorBody)."
                  )
                )
              }
            } else {
              r.entity.dataBytes.runWith(Sink.ignore)
              val etag = r.headers.find(_.lowercaseName() == "etag").map(_.value)
              etag
                .map(t => Future.successful(SuccessfulUploadPart(upload, index, t)))
                .getOrElse(
                  Future.successful(FailedUploadPart(upload, index, new RuntimeException(s"Cannot find etag in ${r}")))
                )
            }

          case (Failure(e), (upload, index)) => Future.successful(FailedUploadPart(upload, index, e))
        }
    }.mapMaterializedValue(_ => NotUsed)
  }

  private def completionSink[T](
      s3Location: S3Location,
      transformResult: CompleteMultipartUploadResult => T
  ): Sink[UploadPartResponse, Future[T]] = {
    MaterializerAccess.sink { implicit mat =>
      import mat.executionContext
      Sink.seq[UploadPartResponse].mapMaterializedValue { responseFuture: Future[Seq[UploadPartResponse]] =>
        responseFuture
          .flatMap { responses: Seq[UploadPartResponse] =>
            val successes = responses.collect { case r: SuccessfulUploadPart => r }
            val failures = responses.collect { case r: FailedUploadPart => r }
            if (responses.isEmpty) {
              Future.failed(new RuntimeException("No Responses"))
            } else if (failures.isEmpty) {
              Future.successful(successes.sortBy(_.index))
            } else {
              Future.failed(FailedUpload(failures.map(_.exception)))
            }
          }
          .flatMap(completeMultipartUpload(s3Location, _))
      }.mapMaterializedValue(_.map(transformResult))
    }.mapMaterializedValue(_.flatten)
  }

  private def signAndGetAs[T](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T], mat: Materializer): Future[T] = {
    import mat.executionContext
    for {
      response <- signAndRequest(request)
      (entity, _) <- entityForSuccess(response)
      t <- Unmarshal(entity).to[T]
    } yield t
  }

  private def signAndGetAs[T](request: HttpRequest,
                              f: (T, Seq[HttpHeader]) => T)(implicit um: Unmarshaller[ResponseEntity, T], mat: Materializer): Source[T, NotUsed] = {
    import mat.executionContext
    signAndRequest(request)
      .mapAsync(parallelism = 1)(entityForSuccess)
      .mapAsync(parallelism = 1) {
        case (entity, headers) => Unmarshal(entity).to[T].map((_, headers))
      }
      .map(f.tupled)
  }

  private def signAndRequest(request: HttpRequest, retries: Int = 3): Source[HttpResponse, NotUsed] =
    ActorSystemAccess.source { implicit system =>
      implicit val conf = S3Ext(system).settings

      Signer.signedRequest(request, signingKey)
        .mapAsync(parallelism = 1)(req => Http().singleRequest(req))
        .flatMapConcat {
          case HttpResponse(status, _, _, _) if (retries > 0) && (500 to 599 contains status.intValue()) =>
            signAndRequest(request, retries - 1)
          case res => Source.single(res)
        }
    }.mapMaterializedValue(_ => NotUsed)

  private def entityForSuccess(
      resp: HttpResponse
  )(implicit mat: Materializer): Future[(ResponseEntity, Seq[HttpHeader])] = {
    import mat.executionContext
    resp match {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful((entity, headers))
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
        }
    }
  }

  private[impl] def createPartitions(chunkSize: Int,
                                     sourceLocation: S3Location)(objectSize: Long): List[CopyPartition] =
    if (objectSize <= 0 || objectSize < chunkSize) CopyPartition(1, sourceLocation) :: Nil
    else {
      ((0L until objectSize by chunkSize).toList :+ objectSize)
        .sliding(2)
        .toList
        .zipWithIndex
        .map {
          case (ls, index) => CopyPartition(index + 1, sourceLocation, Some(ByteRange(ls.head, ls.last)))
        }
    }

  private def createCopyRequests(
      location: S3Location,
      sourceVersionId: Option[String],
      contentType: ContentType,
      s3Headers: S3Headers,
      sse: Option[ServerSideEncryption],
      partitions: Source[List[CopyPartition], NotUsed]
  )(parallelism: Int) = {
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] =
      initiateUpload(location,
                     contentType,
                     S3Headers(
                       s3Headers.headers ++
                       sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(InitiateMultipartUpload) }
                     ))

    val headers: S3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(CopyPart) })

    MaterializerAccess.source { mat =>
      implicit val conf = S3Ext(mat.system).settings

      requestInfo
        .zipWith(partitions) {
          case ((upload, _), ls) =>
            ls.map { cp =>
              val multipartCopy = MultipartCopy(upload, cp)
              val request = uploadCopyPartRequest(multipartCopy, sourceVersionId, headers)
              (request, multipartCopy)
            }
        }
        .mapConcat(identity)
        .flatMapConcat {
          case (req, info) => Signer.signedRequest(req, signingKey).zip(Source.single(info))
        }
    }.mapMaterializedValue(_ => NotUsed)
  }

  private def processUploadCopyPartRequests(
      requests: Source[(HttpRequest, MultipartCopy), NotUsed]
  )(parallelism: Int) = {
    MaterializerAccess.source { implicit mat =>
      import mat.executionContext
      implicit val sys = mat.system

      requests
        .via(Http().superPool[MultipartCopy]())
        .map {
          case (Success(r), multipartCopy) =>
            val entity = r.entity
            val upload = multipartCopy.multipartUpload
            val index = multipartCopy.copyPartition.partNumber
            import StatusCodes._
            r.status match {
              case OK =>
                Unmarshal(entity).to[CopyPartResult].map(cp => SuccessfulUploadPart(upload, index, cp.eTag))
              case statusCode: StatusCode =>
                Unmarshal(entity).to[String].map { err =>
                  val response = Option(err).getOrElse(s"Failed to upload part into S3, status code was: $statusCode")
                  throw new S3Exception(response)
                }
            }

          case (Failure(ex), multipartCopy) =>
            Future.successful(FailedUploadPart(multipartCopy.multipartUpload, multipartCopy.copyPartition.partNumber, ex))
        }
        .mapAsync(parallelism)(identity)
    }
  }

}
