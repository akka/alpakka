/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.impl.model.JavaUri
import akka.http.javadsl.model.headers.ByteRange
import akka.http.javadsl.model._
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.http.scaladsl.model.{ContentType => ScalaContentType, HttpMethod => ScalaHttpMethod}
import akka.stream.Materializer
import akka.stream.alpakka.s3.{scaladsl, S3Settings}
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.{AWSCredentials => OldAWSCredentials}
import akka.stream.alpakka.s3.impl._
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.auth._
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

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

object MultipartUploadResult {
  def create(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(JavaUri(r.location), r.bucket, r.key, r.etag)
}

object S3Client {
  def create(system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(ConfigFactory.load()), system, mat)

  @deprecated("use apply(AWSCredentialsProvider, String) factory", "0.11")
  def create(credentials: OldAWSCredentials, region: String)(implicit system: ActorSystem,
                                                             mat: Materializer): S3Client =
    create(
      new AWSStaticCredentialsProvider(credentials.toAmazonCredentials()),
      region
    )

  def create(credentials: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                  mat: Materializer): S3Client = {

    val settings = S3Settings(ConfigFactory.load()).copy(
      credentialsProvider = credentials,
      s3Region = region
    )
    new S3Client(settings, system, mat)
  }
}

final class S3Client(s3Settings: S3Settings, system: ActorSystem, mat: Materializer) {
  private val impl = S3Stream(s3Settings)(system, mat)

  /**
   * Use this to extend the library
   */
  def request(bucket: String, key: String, method: HttpMethod = HttpMethods.GET): CompletionStage[HttpResponse] =
    impl
      .request(S3Location(bucket, key), method.asInstanceOf[ScalaHttpMethod])
      .map(_.asInstanceOf[HttpResponse])(system.dispatcher)
      .toJava

  def getObjectMetadata(bucket: String, key: String): CompletionStage[Optional[ListBucketResultContents]] =
    impl
      .getObjectMetadata(bucket, key)
      .map { opt =>
        Optional.ofNullable(opt.map(metaDataToJava).orNull)
      }(mat.executionContext)
      .toJava

  def deleteObject(bucket: String, key: String): Future[Unit] =
    impl.deleteObject(S3Location(bucket, key))

  def putObject(bucket: String,
                key: String,
                data: ByteString,
                contentType: ContentType,
                s3Headers: S3Headers): CompletionStage[ListBucketResultContents] =
    impl
      .putObject(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], data, s3Headers)
      .map(metaDataToJava)(mat.executionContext)
      .toJava

  def putObject(bucket: String,
                key: String,
                data: ByteString,
                contentType: ContentType,
                cannedAcl: CannedAcl,
                metaHeaders: MetaHeaders): CompletionStage[ListBucketResultContents] =
    putObject(bucket, key, data, contentType, S3Headers(cannedAcl, metaHeaders))

  def putObject(bucket: String,
                key: String,
                data: ByteString,
                contentType: ContentType): CompletionStage[ListBucketResultContents] =
    putObject(bucket, key, data, contentType, CannedAcl.Private, MetaHeaders(Map()))

  def putObject(bucket: String, key: String, data: ByteString): CompletionStage[ListBucketResultContents] =
    putObject(bucket, key, data, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()))

  def download(bucket: String, key: String): Source[ByteString, CompletionStage[ListBucketResultContents]] =
    impl
      .download(S3Location(bucket, key))
      .mapMaterializedValue(_.map(metaDataToJava)(mat.executionContext).toJava)
      .asJava

  def download(bucket: String,
               key: String,
               range: ByteRange): Source[ByteString, CompletionStage[ListBucketResultContents]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    impl
      .download(S3Location(bucket, key), Some(scalaRange))
      .mapMaterializedValue(_.map(metaDataToJava)(mat.executionContext).toJava)
      .asJava
  }

  /**
   * Will return a source of object metadata for a given bucket with optional prefix.
   * This will automatically page through all keys with the given parameters.
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    impl
      .listBucket(bucket, prefix)
      .map { scalaContents =>
        metaDataToJava(scalaContents)
      }
      .asJava

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], s3Headers)
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders))

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()))

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()))

  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()))

  private def metaDataToJava(scalaContents: scaladsl.ListBucketResultContents): ListBucketResultContents =
    ListBucketResultContents(scalaContents.bucketName,
                             scalaContents.key,
                             scalaContents.eTag,
                             scalaContents.size,
                             scalaContents.lastModified,
                             scalaContents.storageClass)
}
