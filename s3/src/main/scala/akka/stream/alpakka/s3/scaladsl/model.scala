/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import java.time.Instant

import akka.http.scaladsl.model.{DateTime, HttpHeader, Uri}
import akka.http.scaladsl.model.headers._
import akka.stream.alpakka.s3.impl.CompleteMultipartUploadResult

import scala.collection.immutable.Seq

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
    case ct: `Content-Type` => ct.value
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
   * Gets the optional Cache-Control header
   */
  lazy val cacheControl: Option[String] = metadata.collectFirst {
    case c: `Cache-Control` => c.value
  }

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
