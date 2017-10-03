/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.alpakka.oracle.bmcs._
import akka.stream.Materializer
import akka.stream.alpakka.oracle.bmcs.auth.{BmcsCredentials, BmcsSigner}
import akka.stream.alpakka.oracle.bmcs.scaladsl.ListObjectsResultContents
import akka.stream.alpakka.s3.impl.{DiskBuffer, MemoryBuffer, SplitAfterSize}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

sealed trait PartUploadResponse {
  def multipartUpload: MultipartUpload

  def index: Int
}

final case class SuccessfulPartUpload(multipartUpload: MultipartUpload, index: Int, etag: String)
    extends PartUploadResponse

final case class FailedPartUpload(multipartUpload: MultipartUpload, index: Int, exception: Throwable)
    extends PartUploadResponse

final case class FailedUpload(reasons: Seq[Throwable]) extends Exception(reasons.map(_.getMessage).mkString(", "))

final case class CompleteMultipartUploadResult(bucket: String, objectName: String, etag: String)

object BmcsStream {
  def apply(settings: BmcsSettings, credentials: BmcsCredentials)(implicit system: ActorSystem,
                                                                  mat: Materializer): BmcsStream =
    new BmcsStream(settings, credentials)
}

class BmcsStream(settings: BmcsSettings, credentials: BmcsCredentials)(implicit system: ActorSystem, mat: Materializer)
    extends BmcsJsonSupport {

  import HttpRequests._

  implicit val conf: BmcsSettings = settings

  val MinChunkSize = 5242880 //in bytes

  def listBucket(bucket: String, prefix: Option[String] = None): Source[ListObjectsResultContents, NotUsed] = {
    sealed trait ListBucketState
    case object Starting extends ListBucketState
    case class Running(nextStart: String) extends ListBucketState
    case object Finished extends ListBucketState

    import system.dispatcher

    def listBucketCall(start: Option[String]): Future[Option[(ListBucketState, Seq[ListObjectsResultContents])]] =
      signAndGetAs[ListObjects](listObjectsRequest(bucket, start, prefix))
        .map { (res: ListObjects) =>
          val contents = res.objects.map(ListObjectsResultContents(_, bucket))
          Some(
            res.nextStartWith
              .fold[(ListBucketState, Seq[ListObjectsResultContents])]((Finished, contents))(
                n => (Running(n), contents)
              )
          )
        }

    Source
      .unfoldAsync[ListBucketState, Seq[ListObjectsResultContents]](Starting) {
        case Finished => Future.successful(None)
        case Starting => listBucketCall(None)
        case Running(st) => listBucketCall(Some(st))
      }
      .mapConcat(identity)
  }

  /**
   *
   * Returns a source emitting the bytes of the requested object.
   *
   * The request out to BMCS is made when the source is materialized.
   * Each materialization will make a new request to storage cloud.
   * Fails with BMCSException.
   *
   * @param bucket     the bucket
   * @param objectName the object to download
   * @param range      optional range of bytes.
   * @return source built from the object in storage.
   */
  def download(bucket: String, objectName: String, range: Option[ByteRange] = None): Source[ByteString, NotUsed] = {
    import mat.executionContext
    Source
      .fromFuture(request(bucket, objectName, range).flatMap(entityForSuccess).map(_.dataBytes))
      .flatMapConcat(identity)
  }

  /**
   * Returns a future httpResponse to download an object.
   *
   * @param objectName  the object to download.
   * @param bucket      the bucket
   * @param rangeOption optional range
   * @return a Future of HttpResponse that download the object from bmcs.
   */
  def request(bucket: String, objectName: String, rangeOption: Option[ByteRange] = None): Future[HttpResponse] = {
    val downloadRequest: HttpRequest = getDownloadRequest(bucket, objectName)
    signAndGet(rangeOption match {
      case Some(range) => downloadRequest.withHeaders(headers.Range(range))
      case _ => downloadRequest
    })
  }

  /**
   * Uploads a stream of ByteStrings to a specified location as a multipart upload.
   * @param bucket the bucket
   * @param objectName object name
   * @param chunkSize size of chunk. (doesn't attempt for exact size, this is a lower limit)
   * @param chunkingParallelism Number of parts that are uploaded to the bucket in parallel.
   * @return
   */
  def multipartUpload(bucket: String,
                      objectName: String,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4): Sink[ByteString, Future[CompleteMultipartUploadResult]] =
    chunkAndRequest(bucket, objectName, chunkSize)(chunkingParallelism)
      .toMat(completionSink(bucket, objectName))(Keep.right)

  /**
   * Initiates the multipart upload.
   * https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/MultipartUpload/CreateMultipartUpload
   *
   * @param objectName the object that's being uploaded as multipart.
   * @param bucket     the bucket to upload to
   * @return The uploadId wrapped in [MultipartUpload]
   */
  private def initiateMultipartUpload(bucket: String, objectName: String): Future[MultipartUpload] = {
    import mat.executionContext
    Marshal(CreateMultipartUploadDetails(objectName))
      .to[RequestEntity]
      .flatMap(
        entity =>
          signAndGetAs[MultipartUpload](
            initiateMultipartUploadRequest(bucket)
              .withEntity(entity)
        )
      )
  }

  private def chunkAndRequest(bucket: String, objectName: String, chunkSize: Int = MinChunkSize)(
      parallelism: Int = 4
  ): Flow[ByteString, PartUploadResponse, NotUsed] = {

    // Multipart upload requests (except for the completion api) are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.
    val requestFlow: Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] =
      createMultipartRequests(bucket, objectName, chunkSize, parallelism)

    // The individual upload part requests are processed here
    requestFlow.via(Http().superPool[(MultipartUpload, Int)]()).map {
      case (Success(r), (upload, index)) =>
        r.entity.dataBytes.runWith(Sink.ignore)
        val etag = r.headers.find(_.lowercaseName() == "etag").map(_.value)
        etag
          .map((t) => SuccessfulPartUpload(upload, index, t))
          .getOrElse(FailedPartUpload(upload, index, BmcsException(r, "Cannot find etag")))

      case (Failure(e), (upload, index)) => FailedPartUpload(upload, index, e)
    }
  }

  private def createMultipartRequests(
      bucket: String,
      objectName: String,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] = {
    assert(
      chunkSize >= MinChunkSize,
      "Chunk size must be at least 5242880B. "
    )
    // First step of the multi part upload process is made.
    //  The response is then used to construct the subsequent individual upload part requests
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] = initiateUpload(bucket, objectName)

    SplitAfterSize(chunkSize)(Flow.apply[ByteString])
      .via(getChunkBuffer(chunkSize)) //creates the chunk
      .concatSubstreams
      .zipWith(requestInfo) {
        case (chunkedPayload, (uploadInfo, chunkIndex)) =>
          //each of the payload requests are created
          val partRequest =
            uploadPartRequest(bucket,
                              objectName,
                              chunkIndex,
                              uploadInfo.uploadId,
                              chunkedPayload.data,
                              chunkedPayload.size)
          (partRequest, (uploadInfo, chunkIndex))
      }
      .mapAsync(chunkingParallelism) {
        case (req, info) => BmcsSigner.signedRequest(req, credentials).zip(Future.successful(info))
      }
  }

  private def completionSink(bucket: String,
                             objectName: String): Sink[PartUploadResponse, Future[CompleteMultipartUploadResult]] = {
    import mat.executionContext

    Sink.seq[PartUploadResponse].mapMaterializedValue { responseFuture: Future[Seq[PartUploadResponse]] =>
      responseFuture
        .flatMap { responses: Seq[PartUploadResponse] =>
          val successes = responses.collect { case r: SuccessfulPartUpload => r }
          val failures = responses.collect { case r: FailedPartUpload => r }
          if (responses.isEmpty) {
            Future.failed(new RuntimeException("No Responses"))
          } else if (failures.isEmpty) {
            Future.successful(successes.sortBy(_.index))
          } else {
            Future.failed(FailedUpload(failures.map(_.exception)))
          }
        }
        .flatMap(completeMultipartUpload(bucket, objectName, _))
    }
  }

  private def completeMultipartUpload(bucket: String,
                                      objectName: String,
                                      parts: Seq[SuccessfulPartUpload]): Future[CompleteMultipartUploadResult] = {
    import mat.executionContext
    val partDetails = parts.map(part => CommitMultipartUploadPartDetails(part.index, part.etag))
    val commitDetails = CommitMultipartUploadDetails(partDetails, None)
    Marshal(commitDetails)
      .to[RequestEntity]
      .flatMap(
        entity =>
          signAndGet(
            completeMultipartUploadRequest(bucket, objectName, parts.head.multipartUpload.uploadId).withEntity(entity)
        )
      )
      .flatMap(etagForSuccess)
      .map(CompleteMultipartUploadResult(bucket, objectName, _)) //todo: add the opc-multipart-md5 to be returned.
  }

  private def initiateUpload(bucket: String, objectName: String): Source[(MultipartUpload, Int), NotUsed] =
    Source
      .single("start upload")
      .mapAsync(1)(_ => initiateMultipartUpload(bucket, objectName))
      .mapConcat(r => Stream.continually(r))
      .zip(Source.fromIterator(() => Iterator.from(1)))

  private def signAndGetAs[T](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] = {
    import mat.executionContext
    for (response <- signAndGet(request);
         entity <- entityForSuccess(response);
         t <- Unmarshal(entity).to[T]) yield t
  }

  private def signAndGet(request: HttpRequest): Future[HttpResponse] = {
    import mat.executionContext
    for (req <- BmcsSigner.signedRequest(request, credentials);
         res <- Http().singleRequest(req)) yield res
  }

  private def entityForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(entity)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(BmcsException(resp, err))
        }
    }

  private def etagForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[String] =
    resp match {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() && !status.isRedirection() =>
        entity.dataBytes.runWith(Sink.ignore)
        val maybeEtag: Option[String] = headers.find(_.lowercaseName() == "etag").map(_.value)
        maybeEtag match {
          case Some(etag) => Future.successful(etag)
          case None => Future.failed(BmcsException(resp, "Didnt get etag back for request. "))
        }

      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(BmcsException(resp, err))
        }
    }

  private def getChunkBuffer(chunkSize: Int) = settings.bufferType match {
    case MemoryBufferType => new MemoryBuffer(chunkSize * 2)
    case d @ DiskBufferType(_) =>
      new DiskBuffer(1, chunkSize * 2, d.path) //set max materializations to 2 if we need sha of content body in future.
  }

}
