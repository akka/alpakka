/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.acl.CannedAcl
import com.amazonaws.util.{Base64, Md5Utils}

import scala.collection.immutable

/**
 * Container for headers used in s3 uploads like acl, server side encryption, storage class,
 * metadata or custom headers for more advanced use cases.
 */
case class S3Headers(headers: Seq[HttpHeader])

case class MetaHeaders(metaHeaders: Map[String, String]) {
  def headers: immutable.Seq[HttpHeader] =
    metaHeaders.map { header =>
      RawHeader(s"x-amz-meta-${header._1}", header._2)
    }(collection.breakOut): immutable.Seq[HttpHeader]
}

/**
 * Convenience apply methods for creation of canned acl, meta, encryption, storage class or custom headers.
 */
object S3Headers {
  val empty = S3Headers(Seq())
  def apply(cannedAcl: CannedAcl): S3Headers = S3Headers(Seq(cannedAcl.header))
  def apply(metaHeaders: MetaHeaders): S3Headers = S3Headers(metaHeaders.headers)
  def apply(cannedAcl: CannedAcl, metaHeaders: MetaHeaders): S3Headers =
    S3Headers(metaHeaders.headers :+ cannedAcl.header)
  def apply(storageClass: StorageClass): S3Headers = S3Headers(Seq(storageClass.header))
  def apply(encryption: ServerSideEncryption): S3Headers = S3Headers(encryption.headers)
  def apply(customHeaders: Map[String, String]): S3Headers =
    S3Headers(customHeaders.map { header =>
      RawHeader(header._1, header._2)
    }(collection.breakOut))
  def apply(cannedAcl: CannedAcl = CannedAcl.Private,
            metaHeaders: MetaHeaders = MetaHeaders(Map()),
            storageClass: StorageClass = StorageClass.Standard,
            encryption: ServerSideEncryption = ServerSideEncryption.AES256,
            customHeaders: Seq[HttpHeader] = Seq()): S3Headers = {
    val headers = metaHeaders.headers ++ encryption.headers ++ customHeaders :+ cannedAcl.header :+ storageClass.header
    S3Headers(headers)
  }
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html
 */
sealed abstract class StorageClass(val storageClass: String) {
  def header: HttpHeader = RawHeader("x-amz-storage-class", storageClass)
}

object StorageClass {
  case object Standard extends StorageClass("STANDARD")
  case object InfrequentAccess extends StorageClass("STANDARD_IA")
  case object Glacier extends StorageClass("GLACIER")
  case object ReducedRedundancy extends StorageClass("REDUCED_REDUNDANCY")

  def apply(cls: String): Option[StorageClass] = cls match {
    case Standard.storageClass => Some(Standard)
    case InfrequentAccess.storageClass => Some(InfrequentAccess)
    case Glacier.storageClass => Some(Glacier)
    case ReducedRedundancy.storageClass => Some(ReducedRedundancy)
  }
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
 */
sealed abstract class ServerSideEncryption {
  def headers: immutable.Seq[HttpHeader]

  def headersFor(request: S3Request): immutable.Seq[HttpHeader]
}

object ServerSideEncryption {

  case object AES256 extends ServerSideEncryption {
    override def headers: immutable.Seq[HttpHeader] = RawHeader("x-amz-server-side-encryption", "AES256") :: Nil

    override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
      case PutObject | InitiateMultipartUpload => headers
      case _ => Nil
    }
  }

  /**
   * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   *
   * @param keyId amazon resource name in the "arn:aws:kms:my-region:my-account-id:key/my-key-id" format.
   * @param context optional base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs
   */
  case class KMS(keyId: String, context: Option[String] = None) extends ServerSideEncryption {

    override def headers: immutable.Seq[HttpHeader] = {
      val baseHeaders = RawHeader("x-amz-server-side-encryption", "aws:kms") ::
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId) ::
      Nil

      val headers = baseHeaders ++ context.map(ctx => RawHeader("x-amz-server-side-encryption-context", ctx)).toSeq
      headers
    }

    override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
      case PutObject | InitiateMultipartUpload => headers
      case _ => Nil
    }
  }

  /**
   * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   * @param key base64-encoded encryption key for Amazon S3 to use to encrypt or decrypt your data.
   * @param md5 optional base64-encoded 128-bit MD5 digest of the encryption key according to RFC 1321.
   */
  case class CustomerKeys(key: String, md5: Option[String] = None) extends ServerSideEncryption {

    override def headers: immutable.Seq[HttpHeader] =
      RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256") ::
      RawHeader("x-amz-server-side-encryption-customer-key", key) ::
      RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5.getOrElse({
        val decodedKey = Base64.decode(key)
        val md5 = Md5Utils.md5AsBase64(decodedKey)
        md5
      })) :: Nil

    override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
      case GetObject | HeadObject | PutObject | InitiateMultipartUpload | UploadPart =>
        headers
      case _ => Nil
    }
  }
}
