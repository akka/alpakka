/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import akka.{Done, NotUsed}
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
import com.amazonaws.regions.AwsRegionProvider
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.collection.immutable

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

/**
 * Modelled after com.amazonaws.services.s3.model.ObjectMetadata
 */
final class ObjectMetadata private[javadsl] (
    private val scalaMetadata: scaladsl.ObjectMetadata
) {

  lazy val headers: java.util.List[HttpHeader] = (scalaMetadata.metadata: immutable.Seq[HttpHeader]).asJava

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
  lazy val getETag: Optional[String] =
    scalaMetadata.eTag.fold(Optional.empty[String]())(Optional.of)

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
  def getContentLength: Long =
    scalaMetadata.contentLength

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
  def getContentType: Optional[String] =
    scalaMetadata.contentType.fold(Optional.empty[String]())(Optional.of)

  /**
   * Gets the value of the Last-Modified header, indicating the date
   * and time at which Amazon S3 last recorded a modification to the
   * associated object.
   *
   * @return The date and time at which Amazon S3 last recorded a modification
   *         to the associated object.
   */
  def getLastModified: DateTime =
    scalaMetadata.lastModified
}

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

  def create(credentialsProvider: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                          mat: Materializer): S3Client =
    create(
      credentialsProvider,
      new AwsRegionProvider {
        def getRegion: String = region
      }
    )

  def create(credentialsProvider: AWSCredentialsProvider,
             regionProvider: AwsRegionProvider)(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val settings = S3Settings(ConfigFactory.load()).copy(
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider
    )

    new S3Client(settings, system, mat)
  }

  def create(s3Settings: S3Settings)(implicit system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(s3Settings, system, mat)
}

final class S3Client(s3Settings: S3Settings, system: ActorSystem, mat: Materializer) {
  private val impl = S3Stream(s3Settings)(system, mat)

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[CompletionStage]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod = HttpMethods.GET,
              s3Headers: S3Headers = S3Headers.empty): CompletionStage[HttpResponse] =
    impl
      .request(S3Location(bucket, key), method.asInstanceOf[ScalaHttpMethod], s3Headers = s3Headers)
      .map(_.asInstanceOf[HttpResponse])(system.dispatcher)
      .toJava

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[CompletionStage]] containing an [[Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String): CompletionStage[Optional[ObjectMetadata]] =
    impl
      .getObjectMetadata(bucket, key, None)
      .map { opt =>
        Optional.ofNullable(opt.map(metaDataToJava).orNull)
      }(mat.executionContext)
      .toJava

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[CompletionStage]] containing an [[Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: ServerSideEncryption): CompletionStage[Optional[ObjectMetadata]] =
    impl
      .getObjectMetadata(bucket, key, Some(sse))
      .map { opt =>
        Optional.ofNullable(opt.map(metaDataToJava).orNull)
      }(mat.executionContext)
      .toJava

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[CompletionStage]] of [[Void]]
   */
  def deleteObject(bucket: String, key: String): CompletionStage[Done] =
    impl.deleteObject(S3Location(bucket, key)).map(_ => Done.getInstance())(mat.executionContext).toJava

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                s3Headers: S3Headers): CompletionStage[ObjectMetadata] =
    impl
      .putObject(S3Location(bucket, key),
                 contentType.asInstanceOf[ScalaContentType],
                 data.asScala,
                 contentLength,
                 s3Headers,
                 None)
      .map(metaDataToJava)(mat.executionContext)
      .toJava

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param sse the server side encryption to use
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                s3Headers: S3Headers,
                sse: ServerSideEncryption): CompletionStage[ObjectMetadata] =
    impl
      .putObject(S3Location(bucket, key),
                 contentType.asInstanceOf[ScalaContentType],
                 data.asScala,
                 contentLength,
                 s3Headers,
                 Some(sse))
      .map(metaDataToJava)(mat.executionContext)
      .toJava

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @return ta [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                cannedAcl: CannedAcl,
                metaHeaders: MetaHeaders): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, S3Headers(cannedAcl, metaHeaders))

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @param sse the server side encryption to use
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                cannedAcl: CannedAcl,
                metaHeaders: MetaHeaders,
                sse: ServerSideEncryption): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, S3Headers(cannedAcl, metaHeaders), sse)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, CannedAcl.Private, MetaHeaders(Map()))

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param sse the server side encryption to use
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                sse: ServerSideEncryption): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, CannedAcl.Private, MetaHeaders(Map()), sse)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long): CompletionStage[ObjectMetadata] =
    putObject(bucket,
              key,
              data,
              contentLength,
              ContentTypes.APPLICATION_OCTET_STREAM,
              CannedAcl.Private,
              MetaHeaders(Map()))

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param sse the server side encryption to use
   * @return a [[CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                sse: ServerSideEncryption): CompletionStage[ObjectMetadata] =
    putObject(bucket,
              key,
              data,
              contentLength,
              ContentTypes.APPLICATION_OCTET_STREAM,
              CannedAcl.Private,
              MetaHeaders(Map()),
              sse)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[Source]] of [[ByteString]] that materializes into a [[CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String, key: String): Source[ByteString, CompletionStage[ObjectMetadata]] =
    impl
      .download(S3Location(bucket, key), None, None)
      .mapMaterializedValue(_.map(metaDataToJava)(mat.executionContext).toJava)
      .asJava

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[Source]] of [[ByteString]] that materializes into a [[CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               sse: ServerSideEncryption): Source[ByteString, CompletionStage[ObjectMetadata]] =
    impl
      .download(S3Location(bucket, key), None, Some(sse))
      .mapMaterializedValue(_.map(metaDataToJava)(mat.executionContext).toJava)
      .asJava

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[ByteRange]] you want to download
   * @return A [[Source]] of [[ByteString]] that materializes into a [[CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String, key: String, range: ByteRange): Source[ByteString, CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    impl
      .download(S3Location(bucket, key), Some(scalaRange), None)
      .mapMaterializedValue(_.map(metaDataToJava)(mat.executionContext).toJava)
      .asJava
  }

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[ByteRange]] you want to download
   * @param sse the server side encryption to use
   * @return A [[Source]] of [[ByteString]] that materializes into a [[CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange,
               sse: ServerSideEncryption): Source[ByteString, CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    impl
      .download(S3Location(bucket, key), Some(scalaRange), Some(sse))
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
        listingToJava(scalaContents)
      }
      .asJava

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param s3Headers any headers you want to add
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], s3Headers)
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], s3Headers, Some(sse))
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @param sse sse the server side encryption to use
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders), sse)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
   * @param sse the server side encryption to use
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()), sse)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[ContentType]]
   * @param sse the server side encryption to use
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()), sse)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return a [[Sink]] that accepts [[ByteString]]'s and materializes to a [[CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()), sse)

  private def listingToJava(scalaContents: scaladsl.ListBucketResultContents): ListBucketResultContents =
    ListBucketResultContents(scalaContents.bucketName,
                             scalaContents.key,
                             scalaContents.eTag,
                             scalaContents.size,
                             scalaContents.lastModified,
                             scalaContents.storageClass)

  private def metaDataToJava(scalaContents: scaladsl.ObjectMetadata): ObjectMetadata =
    new ObjectMetadata(scalaContents)
}
