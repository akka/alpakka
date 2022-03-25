/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.net.InetSocketAddress
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.StatusCodes.{NoContent, NotFound, OK}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{headers => http, _}
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.{ClientTransport, Http}
import akka.stream.alpakka.s3.BucketAccess.{AccessDenied, AccessGranted, NotExists}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.impl.auth.{CredentialScope, Signer, SigningKey}
import akka.stream.scaladsl.{Flow, Keep, RetryFlow, RunnableGraph, Sink, Source, Tcp}
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import akka.{Done, NotUsed}
import software.amazon.awssdk.regions.Region

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** Internal Api */
@InternalApi private[s3] final case class S3Location(bucket: String, key: String) {
  def validate(conf: S3Settings): S3Location = {
    BucketAndKey.validateBucketName(bucket, conf)
    BucketAndKey.validateObjectKey(key, conf)
    this
  }
}

/** Internal Api */
@InternalApi private[impl] final case class CompleteMultipartUploadResult(location: Uri,
                                                                          bucket: String,
                                                                          key: String,
                                                                          eTag: String,
                                                                          versionId: Option[String] = None)

/** Internal Api */
@InternalApi private[impl] final case class ListBucketResult(isTruncated: Boolean,
                                                             continuationToken: Option[String],
                                                             contents: Seq[ListBucketResultContents],
                                                             commonPrefixes: Seq[ListBucketResultCommonPrefixes])

/** Internal Api */
@InternalApi private[impl] final case class ListMultipartUploadContinuationToken(nextKeyMarker: Option[String],
                                                                                 nextUploadIdMarker: Option[String])

/** Internal Api */
@InternalApi private[impl] final case class ListMultipartUploadsResult(
    bucket: String,
    keyMarker: Option[String],
    uploadIdMarker: Option[String],
    nextKeyMarker: Option[String],
    nextUploadIdMarker: Option[String],
    delimiter: Option[String],
    maxUploads: Int,
    isTruncated: Boolean,
    uploads: Seq[ListMultipartUploadResultUploads],
    commonPrefixes: Seq[CommonPrefixes]
) {

  /**
   * The continuation token for listing MultipartUpload is a union of both the nextKeyMarker
   * and the nextUploadIdMarker. Even though both `nextKeyMarker` and `nextUploadIdMarker` should be
   * defined (if applicable), for safety reasons we also handle the case where one is defined and not the other.
   */
  def continuationToken: Option[ListMultipartUploadContinuationToken] =
    (nextKeyMarker, nextUploadIdMarker) match {
      case (None, None) => None
      case (key, uploadId) => Some(ListMultipartUploadContinuationToken(key, uploadId))
    }

}

/** Internal Api */
@InternalApi private[impl] final case class ListObjectVersionContinuationToken(nextKeyMarker: Option[String],
                                                                               nextVersionIdMarker: Option[String])

/** Internal Api */
@InternalApi private[impl] final case class ListObjectVersionsResult(
    bucket: String,
    name: String,
    prefix: Option[String],
    keyMarker: Option[String],
    nextKeyMarker: Option[String],
    versionIdMarker: Option[String],
    nextVersionIdMarker: Option[String],
    delimiter: Option[String],
    maxKeys: Int,
    isTruncated: Boolean,
    versions: Seq[ListObjectVersionsResultVersions],
    commonPrefixes: Seq[CommonPrefixes],
    deleteMarkers: Seq[DeleteMarkers]
) {

  /**
   * The continuation token for listing ObjectVersions is a union of both the nextKeyMarker
   * and the nextUploadIdMarker. Note that its possible that only one of these markers can be
   * defined
   */
  def continuationToken: Option[ListObjectVersionContinuationToken] =
    (nextKeyMarker, nextVersionIdMarker) match {
      case (None, None) => None
      case (key, version) => Some(ListObjectVersionContinuationToken(key, version))
    }
}

/** Internal Api */
@InternalApi private[impl] final case class ListPartsResult(bucket: String,
                                                            key: String,
                                                            uploadId: String,
                                                            partNumberMarker: Option[Int],
                                                            nextPartNumberMarker: Option[Int],
                                                            maxParts: Int,
                                                            isTruncated: Boolean,
                                                            parts: Seq[ListPartsResultParts],
                                                            initiator: Option[AWSIdentity],
                                                            owner: Option[AWSIdentity],
                                                            storageClass: String) {

  /**
   * There is an exception for the ListPartsApi when you reach the last page of pagination, typically the
   * continuationToken (in this case `nextPartNumberMarker`) would be `None`. However specifically for ListPartResult
   * on the last page the `nextPartNumberMarker` is 0. Ontop of this the `parts` will be empty so we use these
   * conditions to check if we are on the last page.
   */
  def continuationToken: Option[Int] =
    if (nextPartNumberMarker.contains(0) || parts.isEmpty)
      None
    else nextPartNumberMarker
}

/** Internal Api */
@InternalApi private[impl] final case class CopyPartResult(lastModified: Instant, eTag: String)

/** Internal Api */
@InternalApi private[impl] final case class CopyPartition(partNumber: Int,
                                                          sourceLocation: S3Location,
                                                          range: Option[ByteRange.Slice] = None)

/** Internal Api */
@InternalApi private[impl] final case class MultipartCopy(multipartUpload: MultipartUpload,
                                                          copyPartition: CopyPartition)

/** Internal Api */
@InternalApi private[s3] object S3Stream {

  import HttpRequests._
  import Marshalling._

  val MinChunkSize: Int = 5 * 1024 * 1024 //in bytes
  val atLeastOneByteString: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].orElse(Source.single(ByteString.empty))

  // def because tokens can expire
  private def signingKey(implicit settings: S3Settings) = {
    val requestDate = ZonedDateTime.now(ZoneOffset.UTC)
    SigningKey(
      requestDate,
      settings.credentialsProvider,
      CredentialScope(requestDate.toLocalDate, settings.s3RegionProvider.getRegion, "s3")
    )
  }

  def download(
      s3Location: S3Location,
      range: Option[ByteRange],
      versionId: Option[String],
      s3Headers: S3Headers
  ): Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] = {
    val headers = s3Headers.headersFor(GetObject)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        issueRequest(s3Location, rangeOption = range, versionId = versionId, s3Headers = headers)(mat, attr)
          .map(response => response.withEntity(response.entity.withoutSizeLimit))
          .mapAsync(parallelism = 1)(entityForSuccess)
          .map {
            case (entity, headers) =>
              Some((entity.dataBytes.mapMaterializedValue(_ => NotUsed), computeMetaData(headers, entity)))
          }
          .recover[Option[(Source[ByteString, NotUsed], ObjectMetadata)]] {
            case e: S3Exception if e.code == "NoSuchKey" => None
          }
      }
  }.mapMaterializedValue(_ => NotUsed)

  /**
   * An ADT that represents the current state of pagination
   */
  sealed trait S3PaginationState[T]

  final case class Starting[T]() extends S3PaginationState[T]

  /**
   * S3 typically does pagination by the use of a continuation token which is a unique pointer that is
   * provided upon each page request that when provided for the next request, only retrieves results
   * **after** that token
   */
  final case class Running[T](continuationToken: T) extends S3PaginationState[T]

  final case class Finished[T]() extends S3PaginationState[T]

  type ListBucketState = S3PaginationState[String]

  def listBucketCall[T](
      bucket: String,
      prefix: Option[String],
      delimiter: Option[String],
      s3Headers: S3Headers,
      token: Option[String],
      resultTransformer: ListBucketResult => T
  )(implicit mat: Materializer, attr: Attributes): Future[Option[(ListBucketState, T)]] = {
    import mat.executionContext
    implicit val conf: S3Settings = resolveSettings(attr, mat.system)

    signAndGetAs[ListBucketResult](
      HttpRequests.listBucket(bucket, prefix, token, delimiter, s3Headers.headersFor(ListBucket))
    ).map { (res: ListBucketResult) =>
      Some(
        res.continuationToken
          .fold[(ListBucketState, T)]((Finished(), resultTransformer(res)))(
            t => (Running(t), resultTransformer(res))
          )
      )
    }
  }

  def listBucket(bucket: String,
                 prefix: Option[String] = None,
                 s3Headers: S3Headers): Source[ListBucketResultContents, NotUsed] = {

    def listBucketCallOnlyContents(token: Option[String])(implicit mat: Materializer, attr: Attributes) =
      listBucketCall(bucket, prefix, None, s3Headers, token, _.contents)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListBucketState, Seq[ListBucketResultContents]](Starting()) {
            case Finished() => Future.successful(None)
            case Starting() => listBucketCallOnlyContents(None)
            case Running(token) => listBucketCallOnlyContents(Some(token))
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def listBucketAndCommonPrefixes(
      bucket: String,
      delimiter: String,
      prefix: Option[String] = None,
      s3Headers: S3Headers
  ): Source[(immutable.Seq[ListBucketResultContents], immutable.Seq[ListBucketResultCommonPrefixes]), NotUsed] = {

    def listBucketCallContentsAndCommonPrefixes(token: Option[String])(implicit mat: Materializer, attr: Attributes) =
      listBucketCall(bucket,
                     prefix,
                     Some(delimiter),
                     s3Headers,
                     token,
                     listBucketResult => (listBucketResult.contents, listBucketResult.commonPrefixes))

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListBucketState, (Seq[ListBucketResultContents], Seq[ListBucketResultCommonPrefixes])](
            Starting()
          ) {
            case Finished() => Future.successful(None)
            case Starting() => listBucketCallContentsAndCommonPrefixes(None)
            case Running(token) => listBucketCallContentsAndCommonPrefixes(Some(token))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  type ListMultipartUploadState = S3PaginationState[ListMultipartUploadContinuationToken]

  def listMultipartUploadCall[T](
      bucket: String,
      prefix: Option[String],
      delimiter: Option[String],
      s3Headers: S3Headers,
      token: Option[ListMultipartUploadContinuationToken],
      resultTransformer: ListMultipartUploadsResult => T
  )(implicit mat: Materializer, attr: Attributes): Future[Option[(ListMultipartUploadState, T)]] = {
    import mat.executionContext
    implicit val conf: S3Settings = resolveSettings(attr, mat.system)

    signAndGetAs[ListMultipartUploadsResult](
      HttpRequests.listMultipartUploads(bucket, prefix, token, delimiter, s3Headers.headersFor(ListBucket))
    ).map { (res: ListMultipartUploadsResult) =>
      Some(
        res.continuationToken
          .fold[(ListMultipartUploadState, T)]((Finished(), resultTransformer(res)))(
            t => (Running(t), resultTransformer(res))
          )
      )
    }
  }

  def listMultipartUploadAndCommonPrefixes(
      bucket: String,
      delimiter: String,
      prefix: Option[String] = None,
      s3Headers: S3Headers
  ): Source[(immutable.Seq[ListMultipartUploadResultUploads], immutable.Seq[CommonPrefixes]), NotUsed] = {

    def listMultipartUploadCallContentsAndCommonPrefixes(
        token: Option[ListMultipartUploadContinuationToken]
    )(implicit mat: Materializer, attr: Attributes) =
      listMultipartUploadCall(bucket,
                              prefix,
                              Some(delimiter),
                              s3Headers,
                              token,
                              listBucketResult => (listBucketResult.uploads, listBucketResult.commonPrefixes))

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListMultipartUploadState, (Seq[ListMultipartUploadResultUploads], Seq[CommonPrefixes])](
            Starting()
          ) {
            case Finished() => Future.successful(None)
            case Starting() => listMultipartUploadCallContentsAndCommonPrefixes(None)
            case Running(token) => listMultipartUploadCallContentsAndCommonPrefixes(Some(token))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def listMultipartUpload(bucket: String,
                          prefix: Option[String] = None,
                          s3Headers: S3Headers): Source[ListMultipartUploadResultUploads, NotUsed] = {

    def listMultipartUploadCallOnlyUploads(
        token: Option[ListMultipartUploadContinuationToken]
    )(implicit mat: Materializer, attr: Attributes) =
      listMultipartUploadCall(bucket, prefix, None, s3Headers, token, _.uploads)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListMultipartUploadState, Seq[ListMultipartUploadResultUploads]](Starting()) {
            case Finished() => Future.successful(None)
            case Starting() => listMultipartUploadCallOnlyUploads(None)
            case Running(token) => listMultipartUploadCallOnlyUploads(Some(token))
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  type ListPartsState = S3PaginationState[Int]

  def listPartsCall[T](
      bucket: String,
      key: String,
      uploadId: String,
      s3Headers: S3Headers,
      token: Option[Int],
      resultTransformer: ListPartsResult => T
  )(implicit mat: Materializer, attr: Attributes): Future[Option[(ListPartsState, T)]] = {
    import mat.executionContext
    implicit val conf: S3Settings = resolveSettings(attr, mat.system)

    signAndGetAs[ListPartsResult](
      HttpRequests.listParts(bucket, key, uploadId, token, s3Headers.headersFor(ListBucket))
    ).map { (res: ListPartsResult) =>
      Some(
        res.continuationToken
          .fold[(ListPartsState, T)]((Finished(), resultTransformer(res)))(
            t => (Running(t), resultTransformer(res))
          )
      )
    }
  }

  def listParts(bucket: String,
                key: String,
                uploadId: String,
                s3Headers: S3Headers): Source[ListPartsResultParts, NotUsed] = {

    def listMultipartUploadCallOnlyUploads(
        token: Option[Int]
    )(implicit mat: Materializer, attr: Attributes) =
      listPartsCall(bucket, key, uploadId, s3Headers, token, _.parts)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListPartsState, Seq[ListPartsResultParts]](Starting()) {
            case Finished() => Future.successful(None)
            case Starting() => listMultipartUploadCallOnlyUploads(None)
            case Running(token) => listMultipartUploadCallOnlyUploads(Some(token))
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  type ListObjectVersionsState = S3PaginationState[ListObjectVersionContinuationToken]

  def listObjectVersionsCall[T](
      bucket: String,
      delimiter: Option[String],
      prefix: Option[String],
      s3Headers: S3Headers,
      token: Option[ListObjectVersionContinuationToken],
      resultTransformer: ListObjectVersionsResult => T
  )(implicit mat: Materializer, attr: Attributes): Future[Option[(ListObjectVersionsState, T)]] = {
    import mat.executionContext
    implicit val conf: S3Settings = resolveSettings(attr, mat.system)

    signAndGetAs[ListObjectVersionsResult](
      HttpRequests.listObjectVersions(bucket, delimiter, prefix, token, s3Headers.headersFor(ListBucket))
    ).map { (res: ListObjectVersionsResult) =>
      Some(
        res.continuationToken
          .fold[(ListObjectVersionsState, T)]((Finished(), resultTransformer(res)))(
            t => (Running(t), resultTransformer(res))
          )
      )
    }
  }

  def listObjectVersionsAndCommonPrefixes(
      bucket: String,
      delimiter: String,
      prefix: Option[String],
      s3Headers: S3Headers
  ): Source[
    (immutable.Seq[ListObjectVersionsResultVersions], immutable.Seq[DeleteMarkers], immutable.Seq[CommonPrefixes]),
    NotUsed
  ] = {

    def listObjectVersionsCallVersionsAndDeleteMarkersAndCommonPrefixes(
        token: Option[ListObjectVersionContinuationToken]
    )(implicit mat: Materializer, attr: Attributes) =
      listObjectVersionsCall(
        bucket,
        Some(delimiter),
        prefix,
        s3Headers,
        token,
        listObjectVersionsResult =>
          (listObjectVersionsResult.versions,
           listObjectVersionsResult.deleteMarkers,
           listObjectVersionsResult.commonPrefixes)
      )

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListObjectVersionsState,
                       (Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers], Seq[CommonPrefixes])](
            Starting()
          ) {
            case Finished() => Future.successful(None)
            case Starting() => listObjectVersionsCallVersionsAndDeleteMarkersAndCommonPrefixes(None)
            case Running(token) => listObjectVersionsCallVersionsAndDeleteMarkersAndCommonPrefixes(Some(token))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def listObjectVersions(
      bucket: String,
      prefix: Option[String],
      s3Headers: S3Headers
  ): Source[(immutable.Seq[ListObjectVersionsResultVersions], immutable.Seq[DeleteMarkers]), NotUsed] = {

    def listObjectVersionsCallOnlyVersions(
        token: Option[ListObjectVersionContinuationToken]
    )(implicit mat: Materializer, attr: Attributes) =
      listObjectVersionsCall(
        bucket,
        None,
        prefix,
        s3Headers,
        token,
        listObjectVersionsResult => (listObjectVersionsResult.versions, listObjectVersionsResult.deleteMarkers)
      )

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        Source
          .unfoldAsync[ListObjectVersionsState, (Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers])](Starting()) {
            case Finished() => Future.successful(None)
            case Starting() => listObjectVersionsCallOnlyVersions(None)
            case Running(token) => listObjectVersionsCallOnlyVersions(Some(token))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Option[String],
                        s3Headers: S3Headers): Source[Option[ObjectMetadata], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        import mat.executionContext
        val headers = s3Headers.headersFor(HeadObject)
        issueRequest(S3Location(bucket, key), HttpMethods.HEAD, versionId = versionId, s3Headers = headers)(mat, attr)
          .flatMapConcat {
            case HttpResponse(OK, headers, entity, _) =>
              Source.future {
                entity.withoutSizeLimit().discardBytes().future().map { _ =>
                  Some(computeMetaData(headers, entity))
                }
              }
            case HttpResponse(NotFound, _, entity, _) =>
              Source.future(entity.discardBytes().future().map(_ => None)(ExecutionContexts.parasitic))
            case response: HttpResponse =>
              Source.future {
                unmarshalError(response.status, response.entity)
              }
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def unmarshalError(code: StatusCode, entity: ResponseEntity)(implicit mat: Materializer) = {
    import mat.executionContext
    Unmarshal(entity).to[String].map { err =>
      throw S3Exception(err, code)
    }
  }

  def deleteObject(s3Location: S3Location, versionId: Option[String], s3Headers: S3Headers): Source[Done, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val m: Materializer = mat

        val headers = s3Headers.headersFor(DeleteObject)
        issueRequest(s3Location, HttpMethods.DELETE, versionId = versionId, s3Headers = headers)(mat, attr)
          .flatMapConcat {
            case HttpResponse(NoContent, _, entity, _) =>
              Source.future(entity.discardBytes().future().map(_ => Done)(ExecutionContexts.parasitic))
            case response: HttpResponse =>
              Source.future {
                unmarshalError(response.status, response.entity)
              }
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  def deleteObjectsByPrefix(bucket: String,
                            prefix: Option[String],
                            deleteAllVersions: Boolean,
                            s3Headers: S3Headers): Source[Done, NotUsed] = {
    val baseDelete = listBucket(bucket, prefix, s3Headers)
      .flatMapConcat(
        listBucketResultContents =>
          deleteObject(S3Location(bucket, listBucketResultContents.key), versionId = None, s3Headers)
      )

    if (deleteAllVersions)
      baseDelete.flatMapConcat { _ =>
        listObjectVersions(bucket, prefix, s3Headers).flatMapConcat {
          case (versions, deleteMarkers) =>
            val allVersions =
              (versions.map(v => (v.key, v.versionId)) ++ deleteMarkers.map(d => (d.key, d.versionId))).distinct
            Source
              .zipN(
                allVersions.map {
                  case (key, versionId) =>
                    deleteObject(S3Location(bucket, key), versionId = versionId, s3Headers)
                }
              )
              .map(_ => Done)
        }
      } else baseDelete
  }

  def putObject(s3Location: S3Location,
                contentType: ContentType,
                data: Source[ByteString, _],
                contentLength: Long,
                s3Headers: S3Headers): Source[ObjectMetadata, NotUsed] = {

    // TODO can we take in a Source[ByteString, NotUsed] without forcing chunking
    // chunked requests are causing S3 to think this is a multipart upload

    val headers = s3Headers.headersFor(PutObject)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        import mat.executionContext
        implicit val sys: ActorSystem = mat.system
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)

        val req = uploadRequest(s3Location, data, contentLength, contentType, headers)

        signAndRequest(req)
          .flatMapConcat {
            case HttpResponse(OK, h, entity, _) =>
              Source.future {
                entity.discardBytes().future().map { _ =>
                  ObjectMetadata(h :+ `Content-Length`(entity.contentLengthOption.getOrElse(0)))
                }
              }
            case response: HttpResponse =>
              Source.future {
                unmarshalError(response.status, response.entity)
              }
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def request(s3Location: S3Location,
              method: HttpMethod = HttpMethods.GET,
              rangeOption: Option[ByteRange] = None,
              versionId: Option[String] = None,
              s3Headers: Seq[HttpHeader] = Seq.empty): Source[HttpResponse, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        issueRequest(s3Location, method, rangeOption, versionId, s3Headers)(mat, attr)
      }
      .mapMaterializedValue(_ => NotUsed)

  private def issueRequest(
      s3Location: S3Location,
      method: HttpMethod = HttpMethods.GET,
      rangeOption: Option[ByteRange] = None,
      versionId: Option[String],
      s3Headers: Seq[HttpHeader]
  )(implicit mat: Materializer, attr: Attributes): Source[HttpResponse, NotUsed] = {
    implicit val sys: ActorSystem = mat.system
    implicit val conf: S3Settings = resolveSettings(attr, sys)
    signAndRequest(requestHeaders(getDownloadRequest(s3Location, method, s3Headers, versionId), rangeOption))
  }

  private def requestHeaders(downloadRequest: HttpRequest, rangeOption: Option[ByteRange]): HttpRequest =
    rangeOption match {
      case Some(range) => downloadRequest.addHeader(http.Range(range))
      case _ => downloadRequest
    }

  private def bucketManagementRequest(bucket: String)(method: HttpMethod, conf: S3Settings): HttpRequest =
    HttpRequests.bucketManagementRequest(S3Location(bucket, key = ""), method)(conf)

  def makeBucketSource(bucket: String, headers: S3Headers): Source[Done, NotUsed] = {
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)

        val region = conf.getS3RegionProvider.getRegion

        // If region is US_EAST_1 then the location constraint is not required
        // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
        val maybeRegionPayload = region match {
          case Region.US_EAST_1 => None
          case region =>
            Some(HttpRequests.createBucketRegionPayload(region)(ExecutionContexts.parasitic))
        }

        s3ManagementRequest[Done](
          bucket = bucket,
          method = HttpMethods.PUT,
          httpRequest = bucketManagementRequest(bucket),
          headers.headersFor(MakeBucket),
          process = processS3LifecycleResponse,
          httpEntity = maybeRegionPayload
        )
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def makeBucket(bucket: String, headers: S3Headers)(implicit mat: Materializer, attr: Attributes): Future[Done] =
    makeBucketSource(bucket, headers).withAttributes(attr).runWith(Sink.ignore)

  def deleteBucketSource(bucket: String, headers: S3Headers): Source[Done, NotUsed] =
    s3ManagementRequest[Done](
      bucket = bucket,
      method = HttpMethods.DELETE,
      httpRequest = bucketManagementRequest(bucket),
      headers.headersFor(DeleteBucket),
      process = processS3LifecycleResponse
    )

  def deleteBucket(bucket: String, headers: S3Headers)(implicit mat: Materializer, attr: Attributes): Future[Done] =
    deleteBucketSource(bucket, headers).withAttributes(attr).runWith(Sink.ignore)

  def checkIfBucketExistsSource(bucketName: String, headers: S3Headers): Source[BucketAccess, NotUsed] =
    s3ManagementRequest[BucketAccess](
      bucket = bucketName,
      method = HttpMethods.HEAD,
      httpRequest = bucketManagementRequest(bucketName),
      headers.headersFor(CheckBucket),
      process = processCheckIfExistsResponse
    )

  def checkIfBucketExists(bucket: String, headers: S3Headers)(implicit mat: Materializer,
                                                              attr: Attributes): Future[BucketAccess] =
    checkIfBucketExistsSource(bucket, headers).withAttributes(attr).runWith(Sink.head)

  private def uploadManagementRequest(bucket: String, key: String, uploadId: String)(method: HttpMethod,
                                                                                     conf: S3Settings): HttpRequest =
    HttpRequests.uploadManagementRequest(S3Location(bucket, key), uploadId, method)(conf)

  def deleteUploadSource(bucket: String, key: String, uploadId: String, headers: S3Headers): Source[Done, NotUsed] =
    s3ManagementRequest[Done](
      bucket = bucket,
      method = HttpMethods.DELETE,
      httpRequest = uploadManagementRequest(bucket, key, uploadId),
      headers.headersFor(DeleteBucket),
      process = processS3LifecycleResponse
    )

  def deleteUpload(bucket: String, key: String, uploadId: String, headers: S3Headers)(implicit mat: Materializer,
                                                                                      attr: Attributes): Future[Done] =
    deleteUploadSource(bucket, key, uploadId, headers).withAttributes(attr).runWith(Sink.ignore)

  private def s3ManagementRequest[T](
      bucket: String,
      method: HttpMethod,
      httpRequest: (HttpMethod, S3Settings) => HttpRequest,
      headers: Seq[HttpHeader],
      process: (HttpResponse, Materializer) => Future[T],
      httpEntity: Option[Future[RequestEntity]] = None
  ): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        implicit val sys: ActorSystem = mat.system
        val conf: S3Settings = resolveSettings(attr, mat.system)

        val baseSource = httpEntity match {
          case Some(requestEntity) =>
            Source.future(requestEntity).flatMapConcat { requestEntity =>
              signAndRequest(
                requestHeaders(
                  httpRequest(method, conf).withEntity(requestEntity),
                  None
                )
              )
            }
          case None =>
            signAndRequest(
              requestHeaders(
                httpRequest(method, conf),
                None
              )
            )
        }

        baseSource.mapAsync(1) { response =>
          process(response, mat)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def processS3LifecycleResponse(response: HttpResponse, materializer: Materializer): Future[Done] = {
    implicit val mat: Materializer = materializer

    response match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        entity.discardBytes().future()
      case response: HttpResponse =>
        unmarshalError(response.status, response.entity)
    }
  }

  private def processCheckIfExistsResponse(response: HttpResponse, materializer: Materializer): Future[BucketAccess] = {
    import materializer.executionContext

    implicit val mat: Materializer = materializer

    response match {
      case code @ HttpResponse(StatusCodes.NotFound | StatusCodes.Forbidden | StatusCodes.OK, _, entity, _) =>
        entity
          .discardBytes()
          .future()
          .map(
            _ =>
              code.status match {
                case StatusCodes.NotFound => NotExists
                case StatusCodes.Forbidden => AccessDenied
                case StatusCodes.OK => AccessGranted
                case other => throw new IllegalArgumentException(s"received status $other")
              }
          )
      case response: HttpResponse =>
        unmarshalError(response.status, response.entity)
    }
  }

  /**
   * Uploads a stream of ByteStrings to a specified location as a multipart upload.
   */
  def multipartUpload(
      s3Location: S3Location,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    chunkAndRequest(s3Location, contentType, s3Headers, chunkSize)(chunkingParallelism)
      .toMat(completionSink(s3Location, s3Headers))(Keep.right)

  /**
   * Uploads a stream of ByteStrings along with a context to a specified location as a multipart upload. The
   * chunkUploadSink parameter allows you to act upon the context when a chunk has been uploaded to S3.
   */
  def multipartUploadWithContext[C](
      s3Location: S3Location,
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] =
    chunkAndRequestWithContext[C](s3Location, contentType, s3Headers, chunkSize, chunkUploadSink)(chunkingParallelism)
      .toMat(completionSink(s3Location, s3Headers))(Keep.right)

  /**
   * Resumes a previously created a multipart upload by uploading a stream of ByteStrings to a specified location
   * and uploadId
   */
  def resumeMultipartUpload(s3Location: S3Location,
                            uploadId: String,
                            previousParts: immutable.Iterable[Part],
                            contentType: ContentType = ContentTypes.`application/octet-stream`,
                            s3Headers: S3Headers,
                            chunkSize: Int = MinChunkSize,
                            chunkingParallelism: Int = 4): Sink[ByteString, Future[MultipartUploadResult]] = {
    val initialUpload = Some((uploadId, previousParts.size + 1))
    val successfulParts = previousParts.map { part =>
      SuccessfulUploadPart(MultipartUpload(s3Location.bucket, s3Location.key, uploadId), part.partNumber, part.eTag)
    }
    chunkAndRequest(s3Location, contentType, s3Headers, chunkSize, initialUpload)(chunkingParallelism)
      .prepend(Source(successfulParts))
      .toMat(completionSink(s3Location, s3Headers))(Keep.right)
  }

  /**
   * Resumes a previously created a multipart upload by uploading a stream of ByteStrings to a specified location
   * and uploadId. The chunkUploadSink parameter allows you to act upon the context when a chunk has been uploaded to
   * S3.
   */
  def resumeMultipartUploadWithContext[C](
      s3Location: S3Location,
      uploadId: String,
      previousParts: immutable.Iterable[Part],
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] = {
    val initialUpload = Some((uploadId, previousParts.size + 1))
    val successfulParts = previousParts.map { part =>
      SuccessfulUploadPart(MultipartUpload(s3Location.bucket, s3Location.key, uploadId), part.partNumber, part.eTag)
    }
    chunkAndRequestWithContext[C](s3Location, contentType, s3Headers, chunkSize, chunkUploadSink, initialUpload)(
      chunkingParallelism
    ).prepend(Source(successfulParts))
      .toMat(completionSink(s3Location, s3Headers))(Keep.right)
  }

  def completeMultipartUpload(
      s3Location: S3Location,
      uploadId: String,
      parts: immutable.Iterable[Part],
      s3Headers: S3Headers
  )(implicit mat: Materializer, attr: Attributes): Future[MultipartUploadResult] = {
    val successfulParts = parts.map { part =>
      SuccessfulUploadPart(MultipartUpload(s3Location.bucket, s3Location.key, uploadId), part.partNumber, part.eTag)
    }
    Source(successfulParts)
      .toMat(completionSink(s3Location, s3Headers).withAttributes(attr))(Keep.right)
      .run()
  }

  private def initiateMultipartUpload(s3Location: S3Location,
                                      contentType: ContentType,
                                      s3Headers: Seq[HttpHeader]): Source[MultipartUpload, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        import mat.executionContext
        implicit val sys: ActorSystem = mat.system
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)

        val req = initiateMultipartUploadRequest(s3Location, contentType, s3Headers)

        signAndRequest(req).flatMapConcat {
          case HttpResponse(status, _, entity, _) if status.isSuccess() =>
            Source.future(Unmarshal(entity).to[MultipartUpload])
          case response: HttpResponse =>
            Source.future {
              unmarshalError(response.status, response.entity)
            }
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  def multipartCopy(
      sourceLocation: S3Location,
      targetLocation: S3Location,
      sourceVersionId: Option[String] = None,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): RunnableGraph[Future[MultipartUploadResult]] = {

    // Pre step get source meta to get content length (size of the object)
    val eventualMaybeObjectSize =
      getObjectMetadata(sourceLocation.bucket, sourceLocation.key, sourceVersionId, s3Headers)
        .map(_.map(_.contentLength))
    val eventualPartitions =
      eventualMaybeObjectSize.map(_.map(createPartitions(chunkSize, sourceLocation)).getOrElse(Nil))

    // Multipart copy upload requests (except for the completion api) are created here.
    //  The initial copy upload request gets executed within this function as well.
    //  The individual copy upload part requests are created.
    val copyRequests =
      createCopyRequests(targetLocation, sourceVersionId, contentType, s3Headers, eventualPartitions)

    // The individual copy upload part requests are processed here
    processUploadCopyPartRequests(copyRequests)(chunkingParallelism)
      .toMat(completionSink(targetLocation, s3Headers))(Keep.right)
  }

  private def computeMetaData(headers: Seq[HttpHeader], entity: ResponseEntity): ObjectMetadata =
    ObjectMetadata(
      headers ++
      Seq(
        `Content-Length`(entity.contentLengthOption.getOrElse(0)),
        `Content-Type`(entity.contentType),
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

  private def completeMultipartUpload(s3Location: S3Location, parts: Seq[SuccessfulUploadPart], s3Headers: S3Headers)(
      implicit mat: Materializer,
      attr: Attributes
  ): Future[CompleteMultipartUploadResult] = {
    def populateResult(result: CompleteMultipartUploadResult,
                       headers: Seq[HttpHeader]): CompleteMultipartUploadResult = {
      val versionId = headers.find(_.lowercaseName() == "x-amz-version-id").map(_.value())
      result.copy(versionId = versionId)
    }

    import mat.executionContext
    implicit val conf: S3Settings = resolveSettings(attr, mat.system)

    val headers = s3Headers.headersFor(UploadPart)

    Source
      .future(
        completeMultipartUploadRequest(parts.head.multipartUpload, parts.map(p => p.partNumber -> p.eTag), headers)
      )
      .flatMapConcat(signAndGetAs[CompleteMultipartUploadResult](_, populateResult(_, _)))
      .runWith(Sink.head)
  }

  /**
   * Initiates a multipart upload. Returns a source of the initiated upload with upload part indicess
   */
  private def initiateUpload(s3Location: S3Location,
                             contentType: ContentType,
                             s3Headers: Seq[HttpHeader]): Source[(MultipartUpload, Int), NotUsed] =
    Source
      .single(s3Location)
      .flatMapConcat(initiateMultipartUpload(_, contentType, s3Headers))
      .mapConcat(r => LazyList.continually(r))
      .zip(Source.fromIterator(() => Iterator.from(1)))

  private def poolSettings(implicit settings: S3Settings, system: ActorSystem) =
    settings.forwardProxy.map(proxy => {
      val address = InetSocketAddress.createUnresolved(proxy.host, proxy.port)
      val transport: ClientTransport = proxy.scheme match {
        case "https" =>
          proxy.credentials.fold(ClientTransport.httpsProxy(address))(
            c => ClientTransport.httpsProxy(address, BasicHttpCredentials(c.username, c.password))
          )
        case "http" =>
          ChangeTargetEndpointTransport(address)
      }
      ConnectionPoolSettings(system).withConnectionSettings(ClientConnectionSettings(system).withTransport(transport))
    })

  private case class ChangeTargetEndpointTransport(address: InetSocketAddress) extends ClientTransport {
    def connectTo(ignoredHost: String, ignoredPort: Int, settings: ClientConnectionSettings)(
        implicit system: ActorSystem
    ): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
      Tcp()
        .outgoingConnection(address,
                            settings.localAddress,
                            settings.socketOptions,
                            halfClose = true,
                            settings.connectingTimeout,
                            settings.idleTimeout)
        .mapMaterializedValue(
          _.map(tcpConn => OutgoingConnection(tcpConn.localAddress, tcpConn.remoteAddress))(system.dispatcher)
        )
  }

  private def singleRequest(req: HttpRequest)(implicit settings: S3Settings, system: ActorSystem) =
    poolSettings.fold(Http().singleRequest(req))(s => Http().singleRequest(req, settings = s))

  private def superPool[T](implicit settings: S3Settings, sys: ActorSystem) =
    poolSettings.fold(Http().superPool[T]())(s => Http().superPool[T](settings = s))

  private def chunkAndRequest(
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int,
      initialUploadState: Option[(String, Int)] = None
  )(parallelism: Int): Flow[ByteString, UploadPartResponse, NotUsed] = {

    def getChunkBuffer(chunkSize: Int, bufferSize: Int, maxRetriesPerChunk: Int)(implicit settings: S3Settings) =
      settings.bufferType match {
        case MemoryBufferType =>
          new MemoryBuffer(bufferSize)
        case d: DiskBufferType =>
          // Number of materializations required will be total number of upload attempts (max retries + 1) multiplied
          // by the number of materializations per attempt (currently once for request signing, and then again for the
          // actual upload).
          new DiskBuffer((maxRetriesPerChunk + 1) * 2, bufferSize, d.path)
      }

    // Multipart upload requests (except for the completion api) are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.

    assert(
      chunkSize >= MinChunkSize,
      s"Chunk size must be at least 5 MB = $MinChunkSize bytes (was $chunkSize bytes). See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html"
    )

    val chunkBufferSize = chunkSize * 2

    val headers = s3Headers.serverSideEncryption.toIndexedSeq.flatMap(_.headersFor(UploadPart))

    Flow
      .fromMaterializer { (mat, attr) =>
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)
        implicit val sys: ActorSystem = mat.system
        implicit val materializer: Materializer = mat

        // Emits at a chunk if no chunks have been emitted. Ensures that we can upload empty files.
        val atLeastOne =
          Flow[Chunk]
            .prefixAndTail(1)
            .flatMapConcat {
              case (prefix, tail) =>
                if (prefix.nonEmpty) {
                  Source(prefix).concat(tail)
                } else {
                  Source.single(MemoryChunk(ByteString.empty))
                }
            }

        val retriableFlow: Flow[(Chunk, (MultipartUpload, Int)), (Try[HttpResponse], (MultipartUpload, Int)), NotUsed] =
          Flow[(Chunk, (MultipartUpload, Int))]
            .map {
              case (chunkedPayload, (uploadInfo, chunkIndex)) =>
                //each of the payload requests are created
                val partRequest =
                  uploadPartRequest(uploadInfo, chunkIndex, chunkedPayload, headers)
                (partRequest, (uploadInfo, chunkIndex))
            }
            .flatMapConcat {
              case (req, info) =>
                Signer.signedRequest(req, signingKey, conf.signAnonymousRequests).zip(Source.single(info))
            }
            .via(superPool[(MultipartUpload, Int)])

        import conf.multipartUploadSettings.retrySettings._

        SplitAfterSize(chunkSize, chunkBufferSize)(atLeastOneByteString)
          .via(getChunkBuffer(chunkSize, chunkBufferSize, maxRetries)) //creates the chunks
          .mergeSubstreamsWithParallelism(parallelism)
          .filter(_.size > 0)
          .via(atLeastOne)
          .zip(requestInfoOrUploadState(s3Location, contentType, s3Headers, initialUploadState))
          .groupBy(parallelism, { case (_, (_, chunkIndex)) => chunkIndex % parallelism })
          // Allow requests that fail with transient errors to be retried, using the already buffered chunk.
          .via(RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, retriableFlow) {
            case (chunkAndUploadInfo, (Success(r), _)) =>
              if (isTransientError(r.status)) {
                r.entity.discardBytes()
                Some(chunkAndUploadInfo)
              } else {
                None
              }
            case (chunkAndUploadInfo, (Failure(_), _)) =>
              // Treat any exception as transient.
              Some(chunkAndUploadInfo)
          })
          .mapAsync(1) {
            case (response, (upload, index)) =>
              handleChunkResponse(response, upload, index, conf.multipartUploadSettings.retrySettings)
          }
          .mergeSubstreamsWithParallelism(parallelism)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def chunkAndRequestWithContext[C](
      s3Location: S3Location,
      contentType: ContentType,
      s3Headers: S3Headers,
      chunkSize: Int,
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      initialUploadState: Option[(String, Int)] = None
  )(parallelism: Int): Flow[(ByteString, C), UploadPartResponse, NotUsed] = {

    // This part of the API doesn't support disk-buffer because we have no way of serializing a C to a ByteString
    // so we only store the chunks in memory
    def getChunk(bufferSize: Int) =
      new MemoryWithContext[C](bufferSize)

    // Multipart upload requests (except for the completion api) are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.

    assert(
      chunkSize >= MinChunkSize,
      s"Chunk size must be at least 5 MB = $MinChunkSize bytes (was $chunkSize bytes). See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html"
    )

    val chunkBufferSize = chunkSize * 2

    val headers = s3Headers.serverSideEncryption.toIndexedSeq.flatMap(_.headersFor(UploadPart))

    Flow
      .fromMaterializer { (mat, attr) =>
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)
        implicit val sys: ActorSystem = mat.system
        implicit val materializer: Materializer = mat

        // Emits at a chunk if no chunks have been emitted. Ensures that we can upload empty files.
        val atLeastOne =
          Flow[(Chunk, immutable.Iterable[C])]
            .prefixAndTail(1)
            .flatMapConcat {
              case (prefix, tail) =>
                if (prefix.nonEmpty) {
                  Source(prefix).concat(tail)
                } else {
                  Source.single((MemoryChunk(ByteString.empty), immutable.Iterable.empty))
                }
            }

        val retriableFlow: Flow[((Chunk, (MultipartUpload, Int)), immutable.Iterable[C]),
                                ((Try[HttpResponse], (MultipartUpload, Int)), immutable.Iterable[C]),
                                NotUsed] =
          Flow[((Chunk, (MultipartUpload, Int)), immutable.Iterable[C])]
            .map {
              case ((chunkedPayload, (uploadInfo, chunkIndex)), allContext) =>
                //each of the payload requests are created
                val partRequest =
                  uploadPartRequest(uploadInfo, chunkIndex, chunkedPayload, headers)
                ((partRequest, (uploadInfo, chunkIndex)), allContext)
            }
            .flatMapConcat {
              case ((req, info), allContext) =>
                Signer.signedRequest(req, signingKey, conf.signAnonymousRequests).zip(Source.single(info)).map {
                  case (httpRequest, data) => (httpRequest, (data, allContext))
                }
            }
            .via(superPool[((MultipartUpload, Int), immutable.Iterable[C])])
            .map {
              case (response, (info, allContext)) => ((response, info), allContext)
            }

        import conf.multipartUploadSettings.retrySettings._

        val atLeastOneByteStringAndEmptyContext: Flow[(ByteString, C), (ByteString, C), NotUsed] =
          Flow[(ByteString, C)].orElse(
            Source.single((ByteString.empty, null.asInstanceOf[C]))
          )

        SplitAfterSizeWithContext(chunkSize)(atLeastOneByteStringAndEmptyContext)
          .via(getChunk(chunkBufferSize))
          .mergeSubstreamsWithParallelism(parallelism)
          .filter { case (chunk, _) => chunk.size > 0 }
          .via(atLeastOne)
          .zip(requestInfoOrUploadState(s3Location, contentType, s3Headers, initialUploadState))
          .groupBy(parallelism, { case (_, (_, chunkIndex)) => chunkIndex % parallelism })
          .map {
            case ((chunk, allContext), info) =>
              ((chunk, info), allContext)
          }
          // Allow requests that fail with transient errors to be retried, using the already buffered chunk.
          .via(RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, retriableFlow) {
            case ((chunkAndUploadInfo, allContext), ((Success(r), _), _)) =>
              if (isTransientError(r.status)) {
                r.entity.discardBytes()
                Some((chunkAndUploadInfo, allContext))
              } else {
                None
              }
            case ((chunkAndUploadInfo, allContext), ((Failure(_), _), _)) =>
              // Treat any exception as transient.
              Some((chunkAndUploadInfo, allContext))
          })
          .mapAsync(1) {
            case ((response, (upload, index)), allContext) =>
              handleChunkResponse(response, upload, index, conf.multipartUploadSettings.retrySettings).map { result =>
                (result, allContext)
              }(ExecutionContexts.parasitic)
          }
          .alsoTo(chunkUploadSink)
          .map { case (result, _) => result }
          .mergeSubstreamsWithParallelism(parallelism)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def requestInfoOrUploadState(s3Location: S3Location,
                                       contentType: ContentType,
                                       s3Headers: S3Headers,
                                       initialUploadState: Option[(String, Int)]) = {
    initialUploadState match {
      case Some((uploadId, initialIndex)) =>
        // We are resuming from a previously aborted Multipart upload so rather than creating a new MultipartUpload
        // resource we just need to set up the initial state
        Source
          .single(s3Location)
          .flatMapConcat(_ => Source.single(MultipartUpload(s3Location.bucket, s3Location.key, uploadId)))
          .mapConcat(r => LazyList.continually(r))
          .zip(Source.fromIterator(() => Iterator.from(initialIndex)))
      case None =>
        // First step of the multi part upload process is made.
        //  The response is then used to construct the subsequent individual upload part requests
        initiateUpload(s3Location, contentType, s3Headers.headersFor(InitiateMultipartUpload))
    }
  }

  private def handleChunkResponse(response: Try[HttpResponse],
                                  upload: MultipartUpload,
                                  index: Int,
                                  retrySettings: RetrySettings)(
      implicit mat: Materializer
  ) = {

    import mat.executionContext

    response match {
      case Success(r) if r.status.isFailure() =>
        val retryInfo =
          if (isTransientError(r.status)) s" after exhausting all retry attempts (settings: $retrySettings)" else ""

        Unmarshal(r.entity).to[String].map { errorBody =>
          FailedUploadPart(
            upload,
            index,
            new RuntimeException(
              s"Upload part $index request failed$retryInfo. Response header: ($r), response body: ($errorBody)."
            )
          )
        }
      case Success(r) =>
        r.entity.discardBytes()
        val eTag = r.headers.find(_.lowercaseName() == "etag").map(_.value)
        eTag
          .map(t => Future.successful(SuccessfulUploadPart(upload, index, t)))
          .getOrElse(
            Future.successful(FailedUploadPart(upload, index, new RuntimeException(s"Cannot find ETag in $r")))
          )

      case Failure(e) => Future.successful(FailedUploadPart(upload, index, e))
    }
  }

  private def isTransientError(status: StatusCode): Boolean = {
    // 5xx errors from S3 can be treated as transient.
    status.intValue >= 500
  }

  private def completionSink(
      s3Location: S3Location,
      s3Headers: S3Headers
  ): Sink[UploadPartResponse, Future[MultipartUploadResult]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        implicit val attributes: Attributes = attr
        import mat.executionContext
        Sink
          .seq[UploadPartResponse]
          .mapMaterializedValue { responseFuture: Future[Seq[UploadPartResponse]] =>
            responseFuture
              .flatMap { responses: Seq[UploadPartResponse] =>
                val successes = responses.collect { case r: SuccessfulUploadPart => r }
                val failures = responses.collect { case r: FailedUploadPart => r }
                if (responses.isEmpty) {
                  Future.failed(new RuntimeException("No Responses"))
                } else if (failures.isEmpty) {
                  Future.successful(successes.sortBy(_.partNumber))
                } else {
                  Future.failed(FailedUpload(failures.map(_.exception)))
                }
              }
              .flatMap(completeMultipartUpload(s3Location, _, s3Headers))
          }
          .mapMaterializedValue(_.map(r => MultipartUploadResult(r.location, r.bucket, r.key, r.eTag, r.versionId)))
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.parasitic))

  private def signAndGetAs[T](
      request: HttpRequest
  )(implicit um: Unmarshaller[ResponseEntity, T], mat: Materializer, attr: Attributes): Future[T] = {
    import mat.executionContext
    implicit val sys: ActorSystem = mat.system
    for {
      response <- signAndRequest(request).runWith(Sink.head)
      (entity, _) <- entityForSuccess(response)
      t <- Unmarshal(entity).to[T]
    } yield t
  }

  private def signAndGetAs[T](
      request: HttpRequest,
      f: (T, Seq[HttpHeader]) => T
  )(implicit um: Unmarshaller[ResponseEntity, T], mat: Materializer, attr: Attributes): Source[T, NotUsed] = {
    import mat.executionContext
    implicit val sys: ActorSystem = mat.system
    signAndRequest(request)
      .mapAsync(parallelism = 1)(entityForSuccess)
      .mapAsync(parallelism = 1) {
        case (entity, headers) => Unmarshal(entity).to[T].map((_, headers))
      }
      .map(f.tupled)
  }

  private def signAndRequest(
      request: HttpRequest
  )(implicit sys: ActorSystem, mat: Materializer, attr: Attributes): Source[HttpResponse, NotUsed] = {
    implicit val conf: S3Settings = resolveSettings(attr, sys)
    import conf.retrySettings._
    import mat.executionContext

    val retriableFlow = Flow[HttpRequest]
      .flatMapConcat(req => Signer.signedRequest(req, signingKey, conf.signAnonymousRequests))
      .mapAsync(parallelism = 1)(
        req =>
          singleRequest(req)
            .map(Success.apply)
            .recover[Try[HttpResponse]] {
              case t => Failure(t)
            }
      )

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
      partitions: Source[List[CopyPartition], NotUsed]
  ) = {
    val requestInfo: Source[(MultipartUpload, Int), NotUsed] =
      initiateUpload(location, contentType, s3Headers.headersFor(InitiateMultipartUpload))

    val headers = s3Headers.headersFor(CopyPart)

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val conf: S3Settings = resolveSettings(attr, mat.system)

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
            case (req, info) =>
              Signer.signedRequest(req, signingKey, conf.signAnonymousRequests).zip(Source.single(info))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def processUploadCopyPartRequests(
      requests: Source[(HttpRequest, MultipartCopy), NotUsed]
  )(parallelism: Int) =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer: Materializer = mat
        import mat.executionContext
        implicit val sys: ActorSystem = mat.system
        implicit val settings: S3Settings = resolveSettings(attr, mat.system)

        requests
          .via(superPool[MultipartCopy])
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
                  unmarshalError(statusCode, entity)
              }

            case (Failure(ex), multipartCopy) =>
              Future.successful(
                FailedUploadPart(multipartCopy.multipartUpload, multipartCopy.copyPartition.partNumber, ex)
              )
          }
          .mapAsync(parallelism)(identity)
      }

  private def resolveSettings(attr: Attributes, sys: ActorSystem) =
    attr
      .get[S3SettingsValue]
      .map(_.settings)
      .getOrElse {
        val s3Extension = S3Ext(sys)
        attr
          .get[S3SettingsPath]
          .map(settingsPath => s3Extension.settings(settingsPath.path))
          .getOrElse(s3Extension.settings)
      }
}
