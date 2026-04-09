/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
    // The key is only required for SharedKey/SharedKeyLite auth. For other auth types
    // (anon, sas) the value is the reference.conf placeholder "none", which is not
    // valid base64. An actual user-supplied key that fails to decode should surface
    // as an error rather than be silently dropped.
    val decodedKey =
      if (accountKey.isEmpty || accountKey == "none") Array.empty[Byte]
      else Base64.getDecoder.decode(accountKey)
    new AzureNameKeyCredential(accountName, decodedKey)
  }
}

/** Modelled after BlobProperties in Azure Blob Storage.
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
  def getContentMd5: Optional[String] = contentMd5.toJava

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
  def getETag: Optional[String] = eTag.toJava

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
  def getContentLength: Long = contentLength

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
  def getContentType: Optional[String] = contentType.toJava

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
  def getLastModified: Option[DateTime] = lastModified

  override def toString: String =
    s"ObjectMetadata(contentMd5=$contentMd5,eTag=$eTag,contentLength=$contentLength," +
      s"contentType=$contentType,lastModified=$lastModified)"
}

object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}

/**
 * Represents a blob returned by the List Blobs operation.
 *
 * @param name blob name
 * @param eTag entity tag
 * @param contentLength size in bytes
 * @param contentType MIME type
 * @param lastModified last modified date as an RFC 1123 string
 * @param blobType BlockBlob, PageBlob, or AppendBlob
 */
final class BlobItem private (
    val name: String,
    val eTag: Option[String],
    val contentLength: Long,
    val contentType: Option[String],
    val lastModified: Option[String],
    val blobType: String
) {
  import scala.jdk.OptionConverters._

  /** Java API */
  def getName: String = name

  /** Java API */
  def getETag: Optional[String] = eTag.toJava

  /** Java API */
  def getContentLength: Long = contentLength

  /** Java API */
  def getContentType: Optional[String] = contentType.toJava

  /** Java API */
  def getLastModified: Optional[String] = lastModified.toJava

  /** Java API */
  def getBlobType: String = blobType

  override def toString: String =
    s"BlobItem(name=$name, eTag=$eTag, contentLength=$contentLength, " +
    s"contentType=$contentType, lastModified=$lastModified, blobType=$blobType)"

  override def equals(other: Any): Boolean = other match {
    case that: BlobItem =>
      name == that.name &&
      eTag == that.eTag &&
      contentLength == that.contentLength &&
      contentType == that.contentType &&
      lastModified == that.lastModified &&
      blobType == that.blobType
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(name, eTag, java.lang.Long.valueOf(contentLength), contentType, lastModified, blobType)
}

object BlobItem {

  /** Scala API */
  def apply(name: String,
            eTag: Option[String],
            contentLength: Long,
            contentType: Option[String],
            lastModified: Option[String],
            blobType: String): BlobItem =
    new BlobItem(name, eTag, contentLength, contentType, lastModified, blobType)

  /** Java API */
  def create(name: String,
             eTag: Optional[String],
             contentLength: Long,
             contentType: Optional[String],
             lastModified: Optional[String],
             blobType: String): BlobItem =
    new BlobItem(name, eTag.toScala, contentLength, contentType.toScala, lastModified.toScala, blobType)
}

/** An entry returned by the List Files and Directories operation on Azure File Share. */
sealed trait FileShareEntry {
  def name: String

  /** Java API */
  def getName: String = name
}

/**
 * Represents a file entry returned by the List Files and Directories operation.
 *
 * @param name file name
 * @param contentLength size in bytes
 */
final class ShareFileItem private (val name: String, val contentLength: Long) extends FileShareEntry {

  /** Java API */
  def getContentLength: Long = contentLength

  override def toString: String = s"ShareFileItem(name=$name, contentLength=$contentLength)"

  override def equals(other: Any): Boolean = other match {
    case that: ShareFileItem => name == that.name && contentLength == that.contentLength
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(name, java.lang.Long.valueOf(contentLength))
}

object ShareFileItem {

  /** Scala API */
  def apply(name: String, contentLength: Long): ShareFileItem = new ShareFileItem(name, contentLength)

  /** Java API */
  def create(name: String, contentLength: Long): ShareFileItem = new ShareFileItem(name, contentLength)
}

/**
 * Represents a directory entry returned by the List Files and Directories operation.
 *
 * @param name directory name
 */
final class ShareDirectoryItem private (val name: String) extends FileShareEntry {

  override def toString: String = s"ShareDirectoryItem(name=$name)"

  override def equals(other: Any): Boolean = other match {
    case that: ShareDirectoryItem => name == that.name
    case _ => false
  }

  override def hashCode(): Int = name.hashCode
}

object ShareDirectoryItem {

  /** Scala API */
  def apply(name: String): ShareDirectoryItem = new ShareDirectoryItem(name)

  /** Java API */
  def create(name: String): ShareDirectoryItem = new ShareDirectoryItem(name)
}
