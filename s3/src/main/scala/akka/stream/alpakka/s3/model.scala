/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.util.{Objects, Optional}

import akka.http.scaladsl.model.{DateTime, HttpHeader, IllegalUriException, Uri}
import akka.http.scaladsl.model.headers._
import akka.stream.alpakka.s3.AccessStyle.PathAccessStyle

import scala.collection.immutable.Seq
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

final class MultipartUploadResult private (
    val location: Uri,
    val bucket: String,
    val key: String,
    val etag: String,
    val versionId: Option[String]
) {

  /** Java API */
  def getLocation: akka.http.javadsl.model.Uri = akka.http.javadsl.model.Uri.create(location)

  /** Java API */
  def getBucket: String = bucket

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getEtag: String = etag

  /** Java API */
  def getVersionId: java.util.Optional[String] = versionId.asJava

  def withLocation(value: Uri): MultipartUploadResult = copy(location = value)
  def withBucket(value: String): MultipartUploadResult = copy(bucket = value)
  def withKey(value: String): MultipartUploadResult = copy(key = value)
  def withEtag(value: String): MultipartUploadResult = copy(etag = value)
  def withVersionId(value: String): MultipartUploadResult = copy(versionId = Option(value))

  private def copy(
      location: Uri = location,
      bucket: String = bucket,
      key: String = key,
      etag: String = etag,
      versionId: Option[String] = versionId
  ): MultipartUploadResult = new MultipartUploadResult(
    location = location,
    bucket = bucket,
    key = key,
    etag = etag,
    versionId = versionId
  )

  override def toString =
    "MultipartUploadResult(" +
    s"location=$location," +
    s"bucket=$bucket," +
    s"key=$key," +
    s"etag=$etag," +
    s"versionId=$versionId" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: MultipartUploadResult =>
      Objects.equals(this.location, that.location) &&
      Objects.equals(this.bucket, that.bucket) &&
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.etag, that.etag) &&
      Objects.equals(this.versionId, that.versionId)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(location, bucket, key, etag, versionId)
}

object MultipartUploadResult {

  /** Scala API */
  def apply(
      location: Uri,
      bucket: String,
      key: String,
      etag: String,
      versionId: Option[String]
  ): MultipartUploadResult = new MultipartUploadResult(
    location,
    bucket,
    key,
    etag,
    versionId
  )

  /** Java API */
  def create(
      location: akka.http.javadsl.model.Uri,
      bucket: String,
      key: String,
      etag: String,
      versionId: java.util.Optional[String]
  ): MultipartUploadResult = apply(
    location.asScala(),
    bucket,
    key,
    etag,
    versionId.asScala
  )
}

/**
 * Thrown when multipart upload or multipart copy fails because of a server failure.
 */
final class FailedUpload private (
    val reasons: Seq[Throwable]
) extends Exception(reasons.map(_.getMessage).mkString(", ")) {

  /** Java API */
  def getReasons: java.util.List[Throwable] = reasons.asJava
}

object FailedUpload {

  def apply(reasons: Seq[Throwable]) = new FailedUpload(reasons)

  /** Java API */
  def create(reasons: Seq[Throwable]) = FailedUpload(reasons)
}

/**
 * @param bucketName The name of the bucket in which this object is stored
 * @param key The key under which this object is stored
 * @param eTag Hex encoded MD5 hash of this object's contents, as computed by Amazon S3
 * @param size The size of this object, in bytes
 * @param lastModified The date, according to Amazon S3, when this object was last modified
 * @param storageClass The class of storage used by Amazon S3 to store this object
 */
final class ListBucketResultContents private (
    val bucketName: String,
    val key: String,
    val eTag: String,
    val size: Long,
    val lastModified: java.time.Instant,
    val storageClass: String
) {

  /** Java API */
  def getBucketName: String = bucketName

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getSize: Long = size

  /** Java API */
  def getLastModified: java.time.Instant = lastModified

  /** Java API */
  def getStorageClass: String = storageClass

  def withBucketName(value: String): ListBucketResultContents = copy(bucketName = value)
  def withKey(value: String): ListBucketResultContents = copy(key = value)
  def withETag(value: String): ListBucketResultContents = copy(eTag = value)
  def withSize(value: Long): ListBucketResultContents = copy(size = value)
  def withLastModified(value: java.time.Instant): ListBucketResultContents = copy(lastModified = value)
  def withStorageClass(value: String): ListBucketResultContents = copy(storageClass = value)

  private def copy(
      bucketName: String = bucketName,
      key: String = key,
      eTag: String = eTag,
      size: Long = size,
      lastModified: java.time.Instant = lastModified,
      storageClass: String = storageClass
  ): ListBucketResultContents = new ListBucketResultContents(
    bucketName = bucketName,
    key = key,
    eTag = eTag,
    size = size,
    lastModified = lastModified,
    storageClass = storageClass
  )

  override def toString =
    "ListBucketResultContents(" +
    s"bucketName=$bucketName," +
    s"key=$key," +
    s"eTag=$eTag," +
    s"size=$size," +
    s"lastModified=$lastModified," +
    s"storageClass=$storageClass" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ListBucketResultContents =>
      Objects.equals(this.bucketName, that.bucketName) &&
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.eTag, that.eTag) &&
      Objects.equals(this.size, that.size) &&
      Objects.equals(this.lastModified, that.lastModified) &&
      Objects.equals(this.storageClass, that.storageClass)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(bucketName, key, eTag, Long.box(size), lastModified, storageClass)
}

object ListBucketResultContents {

  /** Scala API */
  def apply(
      bucketName: String,
      key: String,
      eTag: String,
      size: Long,
      lastModified: java.time.Instant,
      storageClass: String
  ): ListBucketResultContents = new ListBucketResultContents(
    bucketName,
    key,
    eTag,
    size,
    lastModified,
    storageClass
  )

  /** Java API */
  def create(
      bucketName: String,
      key: String,
      eTag: String,
      size: Long,
      lastModified: java.time.Instant,
      storageClass: String
  ): ListBucketResultContents = apply(
    bucketName,
    key,
    eTag,
    size,
    lastModified,
    storageClass
  )
}

/**
 * @param bucketName The name of the bucket in which this object is stored
 * @param prefix The common prefix of keys between Prefix and the next occurrence of the string specified by a delimiter.
 */
final class ListBucketResultCommonPrefixes private (
    val bucketName: String,
    val prefix: String
) {

  /** Java API */
  def getBucketName: String = bucketName

  /** Java API */
  def getPrefix: String = prefix

  def withBucketName(value: String): ListBucketResultCommonPrefixes = copy(bucketName = value)
  def withPrefix(value: String): ListBucketResultCommonPrefixes = copy(prefix = value)

  private def copy(
      bucketName: String = bucketName,
      prefix: String = prefix
  ): ListBucketResultCommonPrefixes = new ListBucketResultCommonPrefixes(
    bucketName = bucketName,
    prefix = prefix
  )

  override def toString =
    "ListBucketResultCommonPrefixes(" +
    s"bucketName=$bucketName," +
    s"prefix=$prefix" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ListBucketResultCommonPrefixes =>
      Objects.equals(this.bucketName, that.bucketName) &&
      Objects.equals(this.prefix, that.prefix)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(bucketName, prefix)
}

object ListBucketResultCommonPrefixes {

  /** Scala API */
  def apply(
      bucketName: String,
      prefix: String
  ): ListBucketResultCommonPrefixes = new ListBucketResultCommonPrefixes(
    bucketName,
    prefix
  )

  /** Java API */
  def create(
      bucketName: String,
      prefix: String
  ): ListBucketResultCommonPrefixes = apply(
    bucketName,
    prefix
  )
}

/**
 * Modelled after com.amazonaws.services.s3.model.ObjectMetadata
 *
 * @param metadata the raw http headers
 */
final class ObjectMetadata private (
    val metadata: Seq[HttpHeader]
) {

  /**
   * Java Api
   */
  lazy val headers: java.util.List[akka.http.javadsl.model.HttpHeader] =
    (metadata: immutable.Seq[akka.http.javadsl.model.HttpHeader]).asJava

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
   * Java Api
   *
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
    eTag.asJava

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
   * For more information on the Content-Length HTTP header, see [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]]
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
   * Java Api
   *
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
   * For more information on the Content-Length HTTP header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]]
   * </p>
   *
   * @return The Content-Length HTTP header indicating the size of the
   *         associated object in bytes.
   * @see ObjectMetadata#setContentLength(long)
   */
  def getContentLength: Long =
    contentLength

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
   * For more information on the Content-Type header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]]
   * </p>
   *
   * @return The HTTP Content-Type header, indicating the type of content
   *         stored in the associated S3 object.
   * @see ObjectMetadata#setContentType(String)
   */
  lazy val contentType: Option[String] = metadata.collectFirst {
    case ct: `Content-Type` => ct.value
  }

  /**
   * Java Api
   *
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
   * For more information on the Content-Type header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]]
   * </p>
   *
   * @return The HTTP Content-Type header, indicating the type of content
   *         stored in the associated S3 object.
   * @see ObjectMetadata#setContentType(String)
   */
  def getContentType: Optional[String] =
    contentType.asJava

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
   * Java Api
   *
   * Gets the value of the Last-Modified header, indicating the date
   * and time at which Amazon S3 last recorded a modification to the
   * associated object.
   *
   * @return The date and time at which Amazon S3 last recorded a modification
   *         to the associated object.
   */
  def getLastModified: DateTime =
    lastModified

  /**
   * Gets the optional Cache-Control header
   */
  lazy val cacheControl: Option[String] = metadata.collectFirst {
    case c: `Cache-Control` => c.value
  }

  /**
   * Java Api
   *
   * Gets the optional Cache-Control header
   */
  def getCacheControl: Optional[String] =
    cacheControl.asJava

  /**
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  lazy val versionId: Option[String] = metadata.collectFirst {
    case v if v.lowercaseName() == "x-amz-version-id" => v.value()
  }

  /**
   * Java Api
   *
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  def getVersionId: Optional[String] = versionId.asJava

}
object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}

/**
 * While checking for bucket access those responses are available
 * 1) AccessDenied - User does have permission to perform ListBucket operation, so bucket exits
 * 2) AccessGranted - User doesn't have rights to perform ListBucket but bucket exits
 * 3) NotExists - Bucket doesn't exit
 *
 * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
 */
sealed class BucketAccess

object BucketAccess {
  case object AccessDenied extends BucketAccess
  case object AccessGranted extends BucketAccess
  case object NotExists extends BucketAccess

  val accessDenied: BucketAccess = AccessDenied
  val accessGranted: BucketAccess = AccessGranted
  val notExists: BucketAccess = NotExists
}

/**
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
 */
object BucketAndKey {
  private val bucketRegexPathStyle = "(/\\.\\.)|(\\.\\./)".r
  private val bucketRegexDns = "[^a-z0-9\\-\\.]{1,255}|[\\.]{2,}".r

  def pathStyleValid(bucket: String) = {
    bucketRegexPathStyle.findFirstIn(bucket).isEmpty && ".." != bucket
  }

  def dnsValid(bucket: String) = {
    bucketRegexDns.findFirstIn(bucket).isEmpty
  }

  private[s3] def validateBucketName(bucket: String, conf: S3Settings): Unit = {
    if (conf.accessStyle == PathAccessStyle) {
      if (!pathStyleValid(bucket)) {
        throw IllegalUriException(
          "The bucket name contains sub-dir selection with `..`",
          "Selecting sub-directories with `..` is forbidden (and won't work with non-path-style access)."
        )
      }
    } else {
      bucketRegexDns.findFirstIn(bucket) match {
        case Some(illegalCharacter) =>
          throw IllegalUriException(
            "Bucket name contains non-LDH characters",
            s"The following character is not allowed: $illegalCharacter"
          )
        case None => ()
      }
    }
  }

  def objectKeyValid(key: String): Boolean = !key.split("/").contains("..")

  private[s3] def validateObjectKey(key: String, conf: S3Settings): Unit = {
    if (conf.validateObjectKey && !objectKeyValid(key))
      throw IllegalUriException(
        "The object key contains sub-dir selection with `..`",
        "Selecting sub-directories with `..` is forbidden (see the `validate-object-key` setting)."
      )

  }

}
