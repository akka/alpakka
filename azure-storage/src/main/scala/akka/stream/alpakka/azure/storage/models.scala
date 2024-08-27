/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.http.scaladsl.model.{DateTime, HttpHeader}
import akka.http.scaladsl.model.headers._
import com.typesafe.config.Config

import java.util.{Base64, Optional}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

final case class AzureNameKeyCredential(accountName: String, accountKey: Array[Byte])

object AzureNameKeyCredential {
  def apply(accountName: String, accountKey: String): AzureNameKeyCredential =
    AzureNameKeyCredential(accountName, Base64.getDecoder.decode(accountKey))

  /** Java API */
  def create(accountName: String, accountKey: String): AzureNameKeyCredential =
    AzureNameKeyCredential(accountName, accountKey)

  def apply(config: Config): AzureNameKeyCredential = {
    val accountName = config.getString("account-name", "")
    val accountKey = config.getString("account-key", "")
    if (accountName.isEmpty) throw new RuntimeException("accountName property must be defined")
    AzureNameKeyCredential(accountName, accountKey)
  }
}

/** Modelled after ObjectMetadata in S3.
 *
 * @param metadata
 *   Raw Http headers
 */
final class ObjectMetadata private (val metadata: Seq[HttpHeader]) {

  /** Java Api
   */
  lazy val headers: java.util.List[akka.http.javadsl.model.HttpHeader] =
    (metadata: Seq[akka.http.javadsl.model.HttpHeader]).asJava

  /**
   * Content MD5
   */
  lazy val contentMd5: Option[String] = metadata.collectFirst {
    case e if e.name == "Content-MD5" => removeQuotes(e.value())
  }

  /**
   * Java API
   * Content MD5
   */
  lazy val getContentMd5: Optional[String] = contentMd5.toJava

  /** Gets the hex encoded 128-bit MD5 digest of the associated object according to RFC 1864. This data is used as an
   * integrity check to verify that the data received by the caller is the same data that was sent by Azure Storage.
   * <p> This field represents the hex encoded 128-bit MD5 digest of an object's content as calculated by Azure
   * Storage. The ContentMD5 field represents the base64 encoded 128-bit MD5 digest as calculated on the caller's side.
   * </p>
   *
   * @return
   *   The hex encoded MD5 hash of the content for the associated object as calculated by Azure Storage.
   */
  lazy val eTag: Option[String] = metadata.collectFirst {
    case e: ETag => removeQuotes(e.etag.value)
  }

  /** Java Api
   *
   * Gets the hex encoded 128-bit MD5 digest of the associated object according to RFC 1864. This data is used as an
   * integrity check to verify that the data received by the caller is the same data that was sent by Azure Storage.
   * <p> This field represents the hex encoded 128-bit MD5 digest of an object's content as calculated by Azure
   * Storage. The ContentMD5 field represents the base64 encoded 128-bit MD5 digest as calculated on the caller's side.
   * </p>
   *
   * @return
   *   The hex encoded MD5 hash of the content for the associated object as calculated by Azure Storage.
   */
  lazy val getETag: Optional[String] = eTag.toJava

  /** <p> Gets the Content-Length HTTP header indicating the size of the associated object in bytes. </p> <p> This field
   * is required when uploading objects to Storage, but the Azure Storage Java client will automatically set it when
   * working directly with files. When uploading directly from a stream, set this field if possible. Otherwise the
   * client must buffer the entire stream in order to calculate the content length before sending the data to Azure
   * Storage. </p> <p> For more information on the Content-Length HTTP header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]] </p>
   *
   * @return
   *   The Content-Length HTTP header indicating the size of the associated object in bytes.
   * @see
   *   ObjectMetadata#setContentLength(long)
   */
  lazy val contentLength: Long =
    metadata
      .collectFirst {
        case cl: `Content-Length` =>
          cl.length
      }
      .getOrElse(0)

  /** Java Api
   *
   * <p> Gets the Content-Length HTTP header indicating the size of the associated object in bytes. </p> <p> This field
   * is required when uploading objects to Storage, but the Azure Storage Java client will automatically set it when
   * working directly with files. When uploading directly from a stream, set this field if possible. Otherwise the
   * client must buffer the entire stream in order to calculate the content length before sending the data to Azure
   * Storage. </p> <p> For more information on the Content-Length HTTP header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]] </p>
   *
   * @return
   *   The Content-Length HTTP header indicating the size of the associated object in bytes.
   * @see
   *   ObjectMetadata#setContentLength(long)
   */
  lazy val getContentLength: Long = contentLength

  /** <p> Gets the Content-Type HTTP header, which indicates the type of content stored in the associated object. The
   * value of this header is a standard MIME type. </p> <p> When uploading files, the Azure Storage Java client will
   * attempt to determine the correct content type if one hasn't been set yet. Users are responsible for ensuring a
   * suitable content type is set when uploading streams. If no content type is provided and cannot be determined by
   * the filename, the default content type, "application/octet-stream", will be used. </p> <p> For more information on
   * the Content-Type header, see [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]] </p>
   *
   * @return
   *   The HTTP Content-Type header, indicating the type of content stored in the associated Storage object.
   * @see
   *   ObjectMetadata#setContentType(String)
   */
  lazy val contentType: Option[String] = metadata.collectFirst {
    case ct: `Content-Type` =>
      ct.value
  }

  /** Java Api
   *
   * <p> Gets the Content-Type HTTP header, which indicates the type of content stored in the associated object. The
   * value of this header is a standard MIME type. </p> <p> When uploading files, the Azure Storage Java client will
   * attempt to determine the correct content type if one hasn't been set yet. Users are responsible for ensuring a
   * suitable content type is set when uploading streams. If no content type is provided and cannot be determined by
   * the filename, the default content type, "application/octet-stream", will be used. </p> <p> For more information on
   * the Content-Type header, see [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]] </p>
   *
   * @return
   *   The HTTP Content-Type header, indicating the type of content stored in the associated Storage object.
   * @see
   *   ObjectMetadata#setContentType(String)
   */
  lazy val getContentType: Optional[String] = contentType.toJava

  /** Gets the value of the Last-Modified header, indicating the date and time at which Azure Storage last recorded a
   * modification to the associated object.
   *
   * @return
   * The date and time at which Azure Storage last recorded a modification to the associated object.
   */
  lazy val lastModified: Option[DateTime] = metadata.collectFirst {
    case ct: `Last-Modified` =>
      ct.date
  }

  /** Java Api
   *
   * Gets the value of the Last-Modified header, indicating the date and time at which Azure Storage last recorded a
   * modification to the associated object.
   *
   * @return
   * The date and time at which Azure Storage last recorded a modification to the associated object.
   */
  lazy val getLastModified: Option[DateTime] = lastModified

  override def toString: String =
    s"""ObjectMetadata(
       |contentMd5=$contentMd5
       | eTag=$eTag,
       | contentLength=$contentLength,
       | contentType=$contentType,
       | lastModified=$lastModified
       |)""".stripMargin.replaceAll(System.lineSeparator(), "")
}

object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}
