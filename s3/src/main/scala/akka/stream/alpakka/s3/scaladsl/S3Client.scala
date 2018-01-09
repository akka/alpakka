/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
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

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.location, r.bucket, r.key, r.etag)
}

final case class ListBucketResultContents(
    /** The name of the bucket in which this object is stored */
    bucketName: String,
    /** The key under which this object is stored */
    key: String,
    /** Hex encoded MD5 hash of this object's contents, as computed by Amazon S3 */
    eTag: String,
    /** The size of this object, in bytes */
    size: Long,
    /** The date, according to Amazon S3, when this object was last modified */
    lastModified: Instant,
    /** The class of storage used by Amazon S3 to store this object */
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
    case ct: ContentType => ct.value
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

}
object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}

object S3Client {
  val MinChunkSize: Int = 5242880

  def apply()(implicit system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(system.settings.config))

  @deprecated("use apply(AWSCredentialsProvider, String) factory", "0.11")
  def apply(credentials: OldAWSCredentials, region: String)(implicit system: ActorSystem,
                                                            mat: Materializer): S3Client =
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
   * @param method the [[HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[Future]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod = HttpMethods.GET,
              s3Headers: S3Headers = S3Headers.empty): Future[HttpResponse] =
    impl.request(S3Location(bucket, key), method, s3Headers = s3Headers)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[Future]] containing an [[Option]] that will be [[None]] in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: Option[ServerSideEncryption] = None): Future[Option[ObjectMetadata]] =
    impl.getObjectMetadata(bucket, key, sse)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[Future]] of [[Done]]
   */
  def deleteObject(bucket: String, key: String): Future[Done] =
    impl.deleteObject(S3Location(bucket, key))

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
   * @return a [[Future]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param range [optional] the [[ByteRange]] you want to download
   * @param sse [optional] the server side encryption used on upload
   * @return A [[Source]] of [[ByteString]] that materializes into a [[Future]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: Option[ByteRange] = None,
               sse: Option[ServerSideEncryption] = None): Source[ByteString, Future[ObjectMetadata]] =
    impl.download(S3Location(bucket, key), range, sse)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix.
   * This will automatically page through all keys with the given parameters.
   *
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    impl.listBucket(bucket, prefix)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[Future]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[Future]] of [[MultipartUploadResult]]
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
}
