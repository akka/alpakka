/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import java.nio.file.Paths
import java.time.LocalDate
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.{AWSCredentials, CredentialScope, Signer, SigningKey}
import akka.stream.alpakka.s3.{DiskBufferType, MemoryBufferType, S3Exception, S3Settings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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

final case class FailedUpload(reasons: Seq[Throwable]) extends Exception

final case class CompleteMultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

final case class ListBucketResult(isTruncated: Boolean, continuationToken: Option[String], keys: Seq[String])

object S3Stream {

  def apply(settings: S3Settings)(implicit system: ActorSystem, mat: Materializer): S3Stream =
    new S3Stream(settings)
}

private[alpakka] final class S3Stream(settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) {

  import Marshalling._
  import HttpRequests._

  implicit val conf = settings
  val MinChunkSize = 5242880 //in bytes
  val signingKey = SigningKey(settings.awsCredentials, CredentialScope(LocalDate.now(), settings.s3Region, "s3"))

  def download(s3Location: S3Location, range: Option[ByteRange] = None): Source[ByteString, NotUsed] = {
    import mat.executionContext
    Source.fromFuture(request(s3Location, range).flatMap(entityForSuccess).map(_.dataBytes)).flatMapConcat(identity)
  }

  def listBucket(bucket: String, prefix: Option[String] = None): Source[String, NotUsed] = {
    sealed trait ListBucketState
    case object Starting extends ListBucketState
    case class Running(continuationToken: String) extends ListBucketState
    case object Finished extends ListBucketState

    import system.dispatcher

    def listBucketCall(token: Option[String]): Future[Option[(ListBucketState, Seq[String])]] =
      signAndGetAs[ListBucketResult](HttpRequests.listBucket(bucket, prefix, token))
        .map { (res: ListBucketResult) =>
          Some(
            res.continuationToken
              .fold[(ListBucketState, Seq[String])]((Finished, res.keys))(t => (Running(t), res.keys))
          )
        }

    Source
      .unfoldAsync[ListBucketState, Seq[String]](Starting) {
        case Finished => Future.successful(None)
        case Starting => listBucketCall(None)
        case Running(token) => listBucketCall(Some(token))
      }
      .mapConcat(identity)
  }

  def request(s3Location: S3Location, rangeOption: Option[ByteRange] = None): Future[HttpResponse] = {
    val downloadRequest = getDownloadRequest(s3Location)
    signAndGet(rangeOption match {
      case Some(range) => downloadRequest.withHeaders(headers.Range(range))
      case _ => downloadRequest
    })
  }

  /**
   * Uploads a stream of ByteStrings to a specified location as a multipart upload.
   */
  def multipartUpload(s3Location: S3Location,
                      contentType: ContentType = ContentTypes.`application/octet-stream`,
                      s3Headers: S3Headers,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4): Sink[ByteString, Future[CompleteMultipartUploadResult]] =
    chunkAndRequest(s3Location, contentType, s3Headers, chunkSize)(chunkingParallelism)
      .toMat(completionSink(s3Location))(Keep.right)

  private def initiateMultipartUpload(s3Location: S3Location,
                                      contentType: ContentType,
                                      s3Headers: S3Headers): Future[MultipartUpload] = {
    import mat.executionContext

    val req = initiateMultipartUploadRequest(s3Location, contentType, s3Headers)

    val response = for {
      signedReq <- Signer.signedRequest(req, signingKey)
      response <- Http().singleRequest(signedReq)
    } yield response
    response.flatMap {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Unmarshal(entity).to[MultipartUpload]
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new Exception("Can't initiate upload: " + err))
        }
    }
  }

  private def completeMultipartUpload(s3Location: S3Location,
                                      parts: Seq[SuccessfulUploadPart]): Future[CompleteMultipartUploadResult] = {
    import mat.executionContext

    for (req <- completeMultipartUploadRequest(parts.head.multipartUpload, parts.map(p => p.index -> p.etag));
         res <- signAndGetAs[CompleteMultipartUploadResult](req)) yield res
  }

  /**
   * Initiates a multipart upload. Returns a source of the initiated upload with upload part indicess
   */
  private def initiateUpload(s3Location: S3Location,
                             contentType: ContentType,
                             s3Headers: S3Headers): Source[(MultipartUpload, Int), NotUsed] =
    Source
      .single(s3Location)
      .mapAsync(1)(initiateMultipartUpload(_, contentType, s3Headers))
      .mapConcat(r => Stream.continually(r))
      .zip(Source.fromIterator(() => Iterator.from(1)))

  private def createRequests(
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize,
      parallelism: Int = 4
  ): Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] = {

    assert(
      chunkSize >= MinChunkSize,
      "Chunk size must be at least 5242880B. See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html"
    )

    // First step of the multi part upload process is made.
    //  The response is then used to construct the subsequent individual upload part requests
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] =
      initiateUpload(s3Location, contentType, s3Headers)

    SplitAfterSize(chunkSize)(Flow.apply[ByteString])
      .via(getChunkBuffer(chunkSize)) //creates the chunks
      .concatSubstreams
      .zipWith(requestInfo) {
        case (chunkedPayload, (uploadInfo, chunkIndex)) =>
          //each of the payload requests are created
          val partRequest =
            uploadPartRequest(uploadInfo, chunkIndex, chunkedPayload.data, chunkedPayload.size)
          (partRequest, (uploadInfo, chunkIndex))
      }
      .mapAsync(parallelism) { case (req, info) => Signer.signedRequest(req, signingKey).zip(Future.successful(info)) }
  }

  private def getChunkBuffer(chunkSize: Int) = settings.bufferType match {
    case MemoryBufferType => new MemoryBuffer(chunkSize * 2)
    case DiskBufferType => new DiskBuffer(2, chunkSize * 2, getDiskBufferPath)
  }

  private val getDiskBufferPath = settings.diskBufferPath match {
    case "" => None
    case s => Some(Paths.get(s))
  }

  private def chunkAndRequest(
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize
  )(parallelism: Int = 4): Flow[ByteString, UploadPartResponse, NotUsed] = {

    // Multipart upload requests (except for the completion api) are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.
    val requestFlow = createRequests(s3Location, contentType, s3Headers, chunkSize, parallelism)

    // The individual upload part requests are processed here
    requestFlow.via(Http().superPool[(MultipartUpload, Int)]()).map {
      case (Success(r), (upload, index)) =>
        r.entity.dataBytes.runWith(Sink.ignore)
        val etag = r.headers.find(_.lowercaseName() == "etag").map(_.value)
        etag
          .map((t) => SuccessfulUploadPart(upload, index, t))
          .getOrElse(FailedUploadPart(upload, index, new RuntimeException("Cannot find etag")))

      case (Failure(e), (upload, index)) => FailedUploadPart(upload, index, e)
    }
  }

  private def completionSink(s3Location: S3Location): Sink[UploadPartResponse, Future[CompleteMultipartUploadResult]] = {
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
    }
  }

  private def signAndGetAs[T](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] = {
    import mat.executionContext
    for (response <- signAndGet(request);
         entity <- entityForSuccess(response);
         t <- Unmarshal(entity).to[T]) yield t
  }

  private def signAndGet(request: HttpRequest): Future[HttpResponse] = {
    import mat.executionContext
    for (req <- Signer.signedRequest(request, signingKey);
         res <- Http().singleRequest(req)) yield res
  }

  private def entityForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() => Future.successful(entity)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new S3Exception(err))
        }
    }
}
