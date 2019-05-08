/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.time.{LocalDate, ZoneOffset}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{NoContent, NotFound, OK}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.s3.auth.{CredentialScope, Signer, SigningKey}
import akka.stream.alpakka.s3.scaladsl.{ListBucketResultContents, ObjectMetadata}
import akka.stream.alpakka.s3.{DiskBufferType, MemoryBufferType, S3Exception, S3Settings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
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

final case class CompleteMultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

final case class ListBucketResult(isTruncated: Boolean,
                                  continuationToken: Option[String],
                                  contents: Seq[ListBucketResultContents])

object S3Stream {

  def apply(settings: S3Settings)(implicit system: ActorSystem, mat: Materializer): S3Stream =
    new S3Stream(settings)
}

private[alpakka] final class S3Stream(settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) {

  import HttpRequests._
  import Marshalling._

  implicit val conf = settings
  val MinChunkSize = 5242880 //in bytes
  // def because tokens can expire
  def signingKey = SigningKey(
    settings.credentialsProvider,
    CredentialScope(LocalDate.now(ZoneOffset.UTC), settings.s3RegionProvider.getRegion, "s3")
  )

  def download(s3Location: S3Location,
               range: Option[ByteRange],
               sse: Option[ServerSideEncryption]): (Source[ByteString, NotUsed], Future[ObjectMetadata]) = {
    import mat.executionContext
    val s3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(GetObject) })
    val future = request(s3Location, rangeOption = range, s3Headers = s3Headers)
    val source = Source
      .fromFuture(future.flatMap(entityForSuccess))
      .map(_.dataBytes)
      .flatMapConcat(identity)
    val meta = future.map(resp ⇒ ObjectMetadata(resp.headers))
    (source, meta)
  }

  def listBucket(bucket: String, prefix: Option[String] = None): Source[ListBucketResultContents, NotUsed] = {
    sealed trait ListBucketState
    case object Starting extends ListBucketState
    case class Running(continuationToken: String) extends ListBucketState
    case object Finished extends ListBucketState

    import system.dispatcher

    def listBucketCall(token: Option[String]): Future[Option[(ListBucketState, Seq[ListBucketResultContents])]] =
      signAndGetAs[ListBucketResult](HttpRequests.listBucket(bucket, prefix, token))
        .map { (res: ListBucketResult) =>
          Some(
            res.continuationToken
              .fold[(ListBucketState, Seq[ListBucketResultContents])]((Finished, res.contents))(
                t => (Running(t), res.contents)
              )
          )
        }

    Source
      .unfoldAsync[ListBucketState, Seq[ListBucketResultContents]](Starting) {
        case Finished => Future.successful(None)
        case Starting => listBucketCall(None)
        case Running(token) => listBucketCall(Some(token))
      }
      .mapConcat(identity)
  }

  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: Option[ServerSideEncryption]): Future[Option[ObjectMetadata]] = {
    implicit val ec = mat.executionContext
    val s3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(HeadObject) })
    request(S3Location(bucket, key), HttpMethods.HEAD, s3Headers = s3Headers).flatMap {
      case HttpResponse(OK, headers, entity, _) =>
        entity.discardBytes().future().map { _ =>
          Some(ObjectMetadata(headers))
        }
      case HttpResponse(NotFound, _, entity, _) =>
        entity.discardBytes().future().map(_ => None)
      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
        }
    }
  }

  def deleteObject(s3Location: S3Location): Future[Done] = {
    implicit val ec = mat.executionContext
    request(s3Location, HttpMethods.DELETE).flatMap {
      case HttpResponse(NoContent, _, entity, _) =>
        entity.discardBytes().future().map(_ => Done)
      case HttpResponse(code, _, entity, _) =>
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
        }
    }
  }

  def putObject(s3Location: S3Location,
                contentType: ContentType,
                data: Source[ByteString, _],
                contentLength: Long,
                s3Headers: S3Headers,
                sse: Option[ServerSideEncryption]): Future[ObjectMetadata] = {

    // TODO can we take in a Source[ByteString, NotUsed] without forcing chunking
    // chunked requests are causing S3 to think this is a multipart upload

    implicit val ec: ExecutionContext = mat.executionContext

    val headers = S3Headers(
      s3Headers.headers ++ sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(PutObject) }
    )
    val req = uploadRequest(s3Location, data, contentLength, contentType, headers)

    val resp = for {
      signedRequest <- Signer.signedRequest(req, signingKey)
      resp <- Http().singleRequest(signedRequest)
    } yield resp

    resp.flatMap {
      case HttpResponse(OK, headers, entity, _) =>
        entity.discardBytes().future().map(_ => ObjectMetadata(headers))
      case HttpResponse(code, _, entity, _) =>
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
        }
    }
  }

  def request(s3Location: S3Location,
              method: HttpMethod = HttpMethods.GET,
              rangeOption: Option[ByteRange] = None,
              s3Headers: S3Headers = S3Headers.empty): Future[HttpResponse] =
    signAndGet(requestHeaders(getDownloadRequest(s3Location, method, s3Headers), rangeOption))

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
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
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

    // use the same key for all sub-requests (chunks)
    val key: SigningKey = signingKey

    val headers: S3Headers = S3Headers(sse.fold[Seq[HttpHeader]](Seq.empty) { _.headersFor(UploadPart) })

    SplitAfterSize(chunkSize)(Flow.apply[ByteString])
      .via(getChunkBuffer(chunkSize)) //creates the chunks
      .concatSubstreams
      .zipWith(requestInfo) {
        case (chunkedPayload, (uploadInfo, chunkIndex)) =>
          //each of the payload requests are created
          val partRequest =
            uploadPartRequest(uploadInfo, chunkIndex, chunkedPayload.data, chunkedPayload.size, headers)
          (partRequest, (uploadInfo, chunkIndex))
      }
      .mapAsync(parallelism) { case (req, info) => Signer.signedRequest(req, key).zip(Future.successful(info)) }
  }

  private def getChunkBuffer(chunkSize: Int) = settings.bufferType match {
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

  private def completionSink(
      s3Location: S3Location
  ): Sink[UploadPartResponse, Future[CompleteMultipartUploadResult]] = {
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
    for {
      req <- Signer.signedRequest(request, signingKey)
      res <- Http().singleRequest(req)
    } yield res
  }

  private def entityForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(entity)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].map { err =>
          throw new S3Exception(err)
        }
    }
}
