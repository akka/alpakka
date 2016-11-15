package akka.stream.alpakka.s3.impl

import java.nio.file.Paths
import java.time.LocalDate

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse, ResponseEntity, Uri }
import akka.http.scaladsl.unmarshalling.{ Unmarshal, Unmarshaller }
import akka.stream.{ Attributes, Materializer }
import akka.stream.alpakka.s3.{ MemoryBufferType, DiskBufferType, S3Settings }
import akka.stream.alpakka.s3.auth.{ AWSCredentials, CredentialScope, Signer, SigningKey }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.util.ByteString

import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

final case class S3Location(bucket: String, key: String)

final case class MultipartUpload(s3Location: S3Location, uploadId: String)

sealed trait UploadPartResponse {
  def multipartUpload: MultipartUpload
  def index: Int
}

final case class SuccessfulUploadPart(multipartUpload: MultipartUpload, index: Int, etag: String) extends UploadPartResponse
final case class FailedUploadPart(multipartUpload: MultipartUpload, index: Int, exception: Throwable) extends UploadPartResponse

final case class FailedUpload(reasons: Seq[Throwable]) extends Exception
final case class CompleteMultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

private[alpakka] final class S3Stream(credentials: AWSCredentials, region: String, val settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) {
  import Marshalling._

  val MinChunkSize = 5242880
  val signingKey = SigningKey(credentials, CredentialScope(LocalDate.now(), region, "s3"))

  def this(credentials: AWSCredentials, region: String = "us-east-1")(implicit system: ActorSystem, mat: Materializer) =
    this(credentials, region, S3Settings(system))

  def download(s3Location: S3Location): Source[ByteString, NotUsed] = {
    import mat.executionContext
    Source.fromFuture(signAndGet(HttpRequests.getRequest(s3Location)).map(_.dataBytes))
      .flatMapConcat(identity)
  }

  /**
   * Uploads a stream of ByteStrings to a specified location as a multipart upload.
   *
   * @param s3Location
   * @param chunkSize
   * @param chunkingParallelism
   * @return
   */
  def multipartUpload(s3Location: S3Location, chunkSize: Int = MinChunkSize, chunkingParallelism: Int = 4): Sink[ByteString, Future[CompleteMultipartUploadResult]] = {
    val flow = chunkAndRequest(s3Location, chunkSize)(chunkingParallelism)
    (if (settings.debugLogging)
      flow.log("s3-upload-response")
      .withAttributes(Attributes.logLevels(
        onElement = Logging.DebugLevel,
        onFailure = Logging.WarningLevel,
        onFinish = Logging.InfoLevel
      ))
    else
      flow).toMat(completionSink(s3Location))(Keep.right)
  }

  def initiateMultipartUpload(s3Location: S3Location): Future[MultipartUpload] = {
    import mat.executionContext

    val req = HttpRequests.initiateMultipartUploadRequest(s3Location)
    val response = for {
      signedReq <- Signer.signedRequest(req, signingKey)
      response <- Http().singleRequest(signedReq)
    } yield {
      response
    }
    response.flatMap {
      case HttpResponse(status, _, entity, _) if status.isSuccess() => Unmarshal(entity).to[MultipartUpload]
      case HttpResponse(status, _, entity, _) => Unmarshal(entity).to[String].flatMap {
        case err =>
          Future.failed(new Exception("Can't initiate upload: " + err))
      }
    }
  }

  def completeMultipartUpload(s3Location: S3Location, parts: Seq[SuccessfulUploadPart]): Future[CompleteMultipartUploadResult] = {
    import mat.executionContext

    for (
      req <- HttpRequests.completeMultipartUploadRequest(parts.head.multipartUpload, parts.map { case p => (p.index, p.etag) });
      res <- signAndGetAs[CompleteMultipartUploadResult](req)
    ) yield res
  }

  /**
   * Initiates a multipart upload. Returns a source of the initiated upload with upload part indicess
   *
   * @param s3Location The s3 location to which to upload to
   * @return
   */
  def initiateUpload(s3Location: S3Location): Source[(MultipartUpload, Int), NotUsed] = {
    Source.single(s3Location).mapAsync(1)(initiateMultipartUpload)
      .mapConcat { case r => Stream.continually(r) }
      .zip(Source.fromIterator(() => Iterator.from(1)))
  }

  /**
   * Transforms a flow of ByteStrings into a flow of HTTPRequests to upload to S3.
   *
   * @param s3Location
   * @param chunkSize
   * @param parallelism
   * @return
   */
  def createRequests(s3Location: S3Location, chunkSize: Int = MinChunkSize, parallelism: Int = 4): Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] = {
    assert(chunkSize >= MinChunkSize, "Chunk size must be at least 5242880B. See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html")
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] = initiateUpload(s3Location)

    SplitAfterSize(chunkSize)(Flow.apply[ByteString])
      .via(getChunkBuffer(chunkSize))
      .concatSubstreams
      .zipWith(requestInfo) { case (payload, (uploadInfo, chunkIndex)) => (HttpRequests.uploadPartRequest(uploadInfo, chunkIndex, payload.data, payload.size), (uploadInfo, chunkIndex)) }
      .mapAsync(parallelism) { case (req, info) => Signer.signedRequest(req, signingKey).zip(Future.successful(info)) }
  }

  private def getChunkBuffer(chunkSize: Int) = settings.bufferType match {
    case MemoryBufferType => new MemoryBuffer(chunkSize * 2)
    case DiskBufferType   => new DiskBuffer(2, chunkSize * 2, getDiskBufferPath)
  }

  private val getDiskBufferPath = settings.diskBufferPath match {
    case "" => None
    case s  => Some(Paths.get(s))
  }

  def chunkAndRequest(s3Location: S3Location, chunkSize: Int = MinChunkSize)(parallelism: Int = 4): Flow[ByteString, UploadPartResponse, NotUsed] = {
    createRequests(s3Location, chunkSize, parallelism)
      .via(Http().superPool[(MultipartUpload, Int)]())
      .map {
        case (Success(r), (upload, index)) =>
          r.entity.dataBytes.runWith(Sink.ignore)
          val etag = r.headers.find(_.lowercaseName() == "etag").map(_.value)
          etag.map((t) => SuccessfulUploadPart(upload, index, t)).getOrElse(FailedUploadPart(upload, index, new RuntimeException("Cannot find etag")))

        case (Failure(e), (upload, index)) => FailedUploadPart(upload, index, e)
      }
  }

  def completionSink(s3Location: S3Location): Sink[UploadPartResponse, Future[CompleteMultipartUploadResult]] = {
    import mat.executionContext

    Sink.seq[UploadPartResponse].mapMaterializedValue {
      case responseFuture: Future[Seq[UploadPartResponse]] =>
        responseFuture.flatMap {
          case responses: Seq[UploadPartResponse] =>
            val successes = responses.collect { case r: SuccessfulUploadPart => r }
            val failures = responses.collect { case r: FailedUploadPart => r }
            if (responses.isEmpty) {
              Future.failed(new RuntimeException("No Responses"))
            } else if (failures.isEmpty) {
              Future.successful(successes.sortBy(_.index))
            } else {
              Future.failed(FailedUpload(failures.map(_.exception)))
            }
        }.flatMap(completeMultipartUpload(s3Location, _))
    }
  }

  private def signAndGetAs[T](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] = {
    import mat.executionContext
    signAndGet(request).flatMap(entity => Unmarshal(entity).to[T])
  }

  private def signAndGet(request: HttpRequest): Future[ResponseEntity] = {
    import mat.executionContext
    for (
      req <- Signer.signedRequest(request, signingKey);
      res <- Http().singleRequest(req);
      t <- entityForSuccess(res)
    ) yield t
  }

  private def entityForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[ResponseEntity] = {
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() => Future.successful(entity)
      case HttpResponse(status, _, entity, _) => Unmarshal(entity).to[String].flatMap {
        case err => Future.failed(new Exception("Error: " + err))
      }
    }
  }
}
