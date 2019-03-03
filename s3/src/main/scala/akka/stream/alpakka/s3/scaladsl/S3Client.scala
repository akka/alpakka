/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{`Content-Length`, `Last-Modified`, ByteRange, ETag}
import akka.stream.Materializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.{AWSCredentials => OldAWSCredentials}
import akka.stream.alpakka.s3.impl._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.amazonaws.auth._
import com.amazonaws.regions.AwsRegionProvider

import scala.collection.immutable.Seq
import scala.concurrent.Future

final case class MultipartUploadResult(location: Uri,
                                       bucket: String,
                                       key: String,
                                       etag: String,
                                       versionId: Option[String])

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.location, r.bucket, r.key, r.etag, r.versionId)
}

/**
 * @param bucketName The name of the bucket in which this object is stored
 * @param key The key under which this object is stored
 * @param eTag Hex encoded MD5 hash of this object's contents, as computed by Amazon S3
 * @param size The size of this object, in bytes
 * @param lastModified The date, according to Amazon S3, when this object was last modified
 * @param storageClass The class of storage used by Amazon S3 to store this object
 */
final case class ListBucketResultContents(
    bucketName: String,
    key: String,
    eTag: String,
    size: Long,
    lastModified: Instant,
    storageClass: String
)

/**
 * Modelled after com.amazonaws.services.s3.model.ObjectMetadata
 * @param metadata the raw http headers
 */
final class ObjectMetadata private (
    val metadata: Seq[HttpHeader]
) {

  /**
   * Gets the hex encoded 128-bit MD5 digest of the associated object
   * according to RFC 1864. This data is used as an integrity check to verify
   * that the data received by the caller is the same data that was sent by
   * Amazon S3.
   * <p>
   * This field represents the hex encoded 128-bit MD5 digest of an object's
   * content as calculated by Amazon S3. The ContentMD5 field represents the
   * base64 encoded 128-bit MD5 digest as calculated on the caller's side.
   * </p>
   *
   * @return The hex encoded MD5 hash of the content for the associated object
   *         as calculated by Amazon S3.
   */
  lazy val eTag: Option[String] = metadata.collectFirst {
    case e: ETag => e.etag.value.drop(1).dropRight(1)
  }

  /**
   * <p>
   * Gets the Content-Length HTTP header indicating the size of the
   * associated object in bytes.
   * </p>
   * <p>
   * This field is required when uploading objects to S3, but the AWS S3 Java
   * client will automatically set it when working directly with files. When
   * uploading directly from a stream, set this field if
   * possible. Otherwise the client must buffer the entire stream in
   * order to calculate the content length before sending the data to
   * Amazon S3.
   * </p>
   * <p>
   * For more information on the Content-Length HTTP header, see <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">
   * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>
   * </p>
   *
   * @return The Content-Length HTTP header indicating the size of the
   *         associated object in bytes.
   * @see ObjectMetadata#setContentLength(long)
   */
  lazy val contentLength: Long =
    metadata
      .collectFirst {
        case cl: `Content-Length` => cl.length
      }
      .getOrElse(0)

  /**
   * <p>
   * Gets the Content-Type HTTP header, which indicates the type of content
   * stored in the associated object. The value of this header is a standard
   * MIME type.
   * </p>
   * <p>
   * When uploading files, the AWS S3 Java client will attempt to determine
   * the correct content type if one hasn't been set yet. Users are
   * responsible for ensuring a suitable content type is set when uploading
   * streams. If no content type is provided and cannot be determined by
   * the filename, the default content type, "application/octet-stream", will
   * be used.
   * </p>
   * <p>
   * For more information on the Content-Type header, see <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17">
   * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17</a>
   * </p>
   *
   * @return The HTTP Content-Type header, indicating the type of content
   *         stored in the associated S3 object.
   * @see ObjectMetadata#setContentType(String)
   */
  lazy val contentType: Option[String] = metadata.collectFirst {
    case h if h.lowercaseName() == "content-type" => h.value
  }

  /**
   * Gets the value of the Last-Modified header, indicating the date
   * and time at which Amazon S3 last recorded a modification to the
   * associated object.
   *
   * @return The date and time at which Amazon S3 last recorded a modification
   *         to the associated object.
   */
  lazy val lastModified: DateTime = metadata.collectFirst {
    case ct: `Last-Modified` => ct.date
  }.get

  /**
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  lazy val versionId: Option[String] = metadata.collectFirst {
    case v if v.lowercaseName() == "x-amz-version-id" => v.value()
  }

}
object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}

object S3Client {
  val MinChunkSize: Int = 5242880

  def apply()(implicit system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(system.settings.config))

  @deprecated("use apply(AWSCredentialsProvider, String) factory", "0.11")
  def apply(credentials: OldAWSCredentials, region: String)(implicit system: ActorSystem, mat: Materializer): S3Client =
    apply(
      new AWSStaticCredentialsProvider(credentials.toAmazonCredentials()),
      region
    )

  def apply(credentialsProvider: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                         mat: Materializer): S3Client =
    apply(
      credentialsProvider,
      new AwsRegionProvider {
        def getRegion: String = region
      }
    )

  def apply(credentialsProvider: AWSCredentialsProvider,
            regionProvider: AwsRegionProvider)(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val settings: S3Settings = S3Settings(system.settings.config).copy(
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider
    )

    new S3Client(settings)
  }
}

final class S3Client(val s3Settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) {

  import S3Client._

  private[this] val impl = S3Stream(s3Settings)

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[akka.http.scaladsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return a [[scala.concurrent.Future Future]] containing the raw [[akka.http.scaladsl.model.HttpResponse HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod = HttpMethods.GET,
              versionId: Option[String] = None,
              s3Headers: S3Headers = S3Headers.empty): Future[HttpResponse] =
    impl.request(S3Location(bucket, key), method, versionId = versionId, s3Headers = s3Headers)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[scala.concurrent.Future Future]] containing an [[scala.Option]] that will be [[scala.None]] in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Option[String] = None,
                        sse: Option[ServerSideEncryption] = None): Future[Option[ObjectMetadata]] =
    impl.getObjectMetadata(bucket, key, versionId, sse)

  /**
    *
    * @param bucket
    * @param key
    * @param versionId
    * @return
    */
  def deleteObject(bucket: String, key: String, versionId: Option[String] = None): Future[Done] =
    impl.deleteObject(S3Location(bucket, key), versionId)

  /**
    * Deletes all keys which have the given prefix under the specified bucket
    * @param bucket
    * @param prefix
    * @return
    */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String]): Future[Done] =
    impl.deleteObjectsByPrefix(bucket, prefix)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[scala.concurrent.Future Future]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType = ContentTypes.`application/octet-stream`,
                s3Headers: S3Headers,
                sse: Option[ServerSideEncryption] = None): Future[ObjectMetadata] =
    impl.putObject(S3Location(bucket, key), contentType, data, contentLength, s3Headers, sse)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse [optional] the server side encryption used on upload
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] and a [[scala.concurrent.Future Future]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: Option[ByteRange] = None,
               versionId: Option[String] = None,
               sse: Option[ServerSideEncryption] = None): (Source[ByteString, NotUsed], Future[ObjectMetadata]) =
    impl.download(S3Location(bucket, key), range, versionId, sse)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `akka.stream.alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   *
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    impl.listBucket(bucket, prefix)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType = ContentTypes.`application/octet-stream`,
                      metaHeaders: MetaHeaders = MetaHeaders(Map()),
                      cannedAcl: CannedAcl = CannedAcl.Private,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4,
                      sse: Option[ServerSideEncryption] = None): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        S3Headers(cannedAcl, metaHeaders),
        sse,
        chunkSize,
        chunkingParallelism
      )
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUploadWithHeaders(
      bucket: String,
      key: String,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: Option[S3Headers] = None,
      sse: Option[ServerSideEncryption] = None
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        s3Headers.getOrElse(S3Headers.empty),
        sse,
        chunkSize,
        chunkingParallelism
      )
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

  /**
   * Copy an S3 object from source bucket to target bucket using multi part copy upload.
   *
   * @param sourceBucket source s3 bucket name
   * @param sourceKey    source s3 key
   * @param targetBucket target s3 bucket name
   * @param targetKey    target s3 key
   * @param sourceVersionId optional version id of source object, if the versioning is enabled in source bucket
   * @param contentType  an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse an optional server side encryption key
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return
   */
  def multipartCopy(
      sourceBucket: String,
      sourceKey: String,
      targetBucket: String,
      targetKey: String,
      sourceVersionId: Option[String] = None,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: Option[S3Headers] = None,
      sse: Option[ServerSideEncryption] = None,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): Future[MultipartUploadResult] =
    impl
      .multipartCopy(
        S3Location(sourceBucket, sourceKey),
        S3Location(targetBucket, targetKey),
        sourceVersionId,
        contentType,
        s3Headers.getOrElse(S3Headers.empty),
        sse,
        chunkSize,
        chunkingParallelism
      )
      .run()
      .map(MultipartUploadResult.apply)(system.dispatcher)
}
