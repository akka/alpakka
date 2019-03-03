/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.javadsl.model.headers.ByteRange
import akka.http.javadsl.model._
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.http.scaladsl.model.{ContentType => ScalaContentType, HttpMethod => ScalaHttpMethod}
import akka.japi.{Pair => JPair}
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
import scala.concurrent.Future

final case class MultipartUploadResult(location: Uri,
                                       bucket: String,
                                       key: String,
                                       etag: String,
                                       versionId: Optional[String])

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

  /**
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  def getVersionId: Optional[String] = scalaMetadata.versionId.fold(Optional.empty[String]())(Optional.of)
}

object MultipartUploadResult {
  def create(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(Uri.create(r.location),
                              r.bucket,
                              r.key,
                              r.etag,
                              r.versionId.fold(Optional.empty[String]())(Optional.of))
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
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String, key: String, method: HttpMethod, s3Headers: S3Headers): CompletionStage[HttpResponse] =
    request(bucket, key, Optional.empty(), method, s3Headers)

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              versionId: Optional[String],
              method: HttpMethod = HttpMethods.GET,
              s3Headers: S3Headers = S3Headers.empty): CompletionStage[HttpResponse] =
    impl
      .request(S3Location(bucket, key),
               method.asInstanceOf[ScalaHttpMethod],
               versionId = Option(versionId.orElse(null)),
               s3Headers = s3Headers)
      .map(_.asInstanceOf[HttpResponse])(system.dispatcher)
      .toJava

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String): CompletionStage[Optional[ObjectMetadata]] =
    getObjectMetadata(bucket, key, null)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: ServerSideEncryption): CompletionStage[Optional[ObjectMetadata]] =
    getObjectMetadata(bucket, key, Optional.empty(), sse)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param sse the server side encryption to use
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Optional[String],
                        sse: ServerSideEncryption): CompletionStage[Optional[ObjectMetadata]] =
    impl
      .getObjectMetadata(bucket, key, Option(versionId.orElse(null)), Option(sse))
      .map { opt =>
        Optional.ofNullable(opt.map(metaDataToJava).orNull)
      }(mat.executionContext)
      .toJava

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
   */
  def deleteObject(bucket: String, key: String): CompletionStage[Done] = deleteObject(bucket, key, Optional.empty())

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
   */
  def deleteObject(bucket: String, key: String, versionId: Optional[String]): CompletionStage[Done] =
    impl
      .deleteObject(S3Location(bucket, key), Option(versionId.orElse(null)))
      .map(_ => Done.getInstance())(mat.executionContext)
      .toJava

  /**
    * Deletes all keys under the specified bucket
    *
    * @param bucket
    * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
    */
  def deleteObjectsByPrefix(bucket: String): CompletionStage[Done] = deleteObjectsByPrefix(bucket, Optional.empty())

  /**
    * Deletes all keys which have the given prefix under the specified bucket
    *
    * @param bucket
    * @param prefix
    * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
    */
  def deleteObjectsByPrefix(bucket: String, prefix: Optional[String]): CompletionStage[Done] =
    impl
      .deleteObjectsByPrefix(bucket, Option(prefix.orElse(null)))
      .map(_ => Done.getInstance())(mat.executionContext)
      .toJava

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @return ta [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
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

  private def toJava[M](
      download: (akka.stream.scaladsl.Source[ByteString, M], Future[scaladsl.ObjectMetadata])
  ): JPair[Source[ByteString, M], CompletionStage[ObjectMetadata]] = {
    val (stream, meta) = download
    JPair(stream.asJava, meta.map(metaDataToJava)(mat.executionContext).toJava)
  }

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String, key: String): JPair[Source[ByteString, NotUsed], CompletionStage[ObjectMetadata]] =
    toJava(impl.download(S3Location(bucket, key), None, None, None))

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               sse: ServerSideEncryption): JPair[Source[ByteString, NotUsed], CompletionStage[ObjectMetadata]] =
    toJava(impl.download(S3Location(bucket, key), None, None, Some(sse)))

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange): JPair[Source[ByteString, NotUsed], CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(impl.download(S3Location(bucket, key), Some(scalaRange), None, None))
  }

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange,
               sse: ServerSideEncryption): JPair[Source[ByteString, NotUsed], CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(impl.download(S3Location(bucket, key), Some(scalaRange), None, Some(sse)))
  }

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange,
               versionId: Optional[String],
               sse: ServerSideEncryption): JPair[Source[ByteString, NotUsed], CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(impl.download(S3Location(bucket, key), Option(scalaRange), Option(versionId.orElse(null)), Option(sse)))
  }

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The <code>akka.stream.alpakka.s3.list-bucket-api-version</code> can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   *
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @param sse sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
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
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      sse: ServerSideEncryption): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()), sse)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param sourceVersionId version id of source object, if the versioning is enabled in source bucket
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    contentType: ContentType,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption): CompletionStage[MultipartUploadResult] =
    impl
      .multipartCopy(
        S3Location(sourceBucket, sourceKey),
        S3Location(targetBucket, targetKey),
        Option(sourceVersionId.orElse(null)),
        contentType.asInstanceOf[ScalaContentType],
        s3Headers,
        Option(sse)
      )
      .run()(mat)
      .map(MultipartUploadResult.create)(system.dispatcher)
      .toJava

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param sourceVersionId version id of source object, if the versioning is enabled in source bucket
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  sourceVersionId,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  s3Headers,
                  sse)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    contentType: ContentType,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, Optional.empty(), contentType, s3Headers, sse)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  s3Headers,
                  sse)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  S3Headers.empty,
                  null)

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
