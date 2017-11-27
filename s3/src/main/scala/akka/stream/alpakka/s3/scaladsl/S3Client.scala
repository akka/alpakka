/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import java.time.Instant
import scala.concurrent.Future
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Materializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.{AWSCredentials ⇒ OldAWSCredentials}
import akka.stream.alpakka.s3.impl._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.auth._

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

  def apply(credentials: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                 mat: Materializer): S3Client = {

    val settings: S3Settings = S3Settings(system.settings.config).copy(
      credentialsProvider = credentials,
      s3Region = region
    )
    new S3Client(settings)
  }
}

final class S3Client(val s3Settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) {

  import S3Client._

  private[this] val impl = S3Stream(s3Settings)

  def request(bucket: String, key: String): Future[HttpResponse] = impl.request(S3Location(bucket, key))

  def download(bucket: String, key: String): Source[ByteString, NotUsed] = impl.download(S3Location(bucket, key))

  def download(bucket: String, key: String, range: ByteRange): Source[ByteString, NotUsed] =
    impl.download(S3Location(bucket, key), Some(range))

  /**
   * Will return a source of object metadata for a given bucket with optional prefix.
   * This will automatically page through all keys with the given parameters.
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    impl.listBucket(bucket, prefix)

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType = ContentTypes.`application/octet-stream`,
                      metaHeaders: MetaHeaders = MetaHeaders(Map()),
                      cannedAcl: CannedAcl = CannedAcl.Private,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        S3Headers(cannedAcl, metaHeaders),
        chunkSize,
        chunkingParallelism
      )
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

  def multipartUploadWithHeaders(
      bucket: String,
      key: String,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: Option[S3Headers] = None
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        s3Headers.getOrElse(S3Headers.empty),
        chunkSize,
        chunkingParallelism
      )
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

}
