/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.headers
import java.util.Objects

import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.impl._
import software.amazon.awssdk.utils.BinaryUtils
import software.amazon.awssdk.utils.Md5Utils

import scala.collection.immutable

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
 */
sealed abstract class ServerSideEncryption {
  @InternalApi private[s3] def headers: immutable.Seq[HttpHeader]

  @InternalApi private[s3] def headersFor(request: S3Request): immutable.Seq[HttpHeader]
}

object ServerSideEncryption {
  def aes256() =
    new AES256()

  def kms(keyId: String) =
    new KMS(keyId, None)

  def customerKeys(key: String) =
    new CustomerKeys(key)
}

final class AES256 private[headers] () extends ServerSideEncryption {
  @InternalApi private[s3] override def headers: immutable.Seq[HttpHeader] =
    RawHeader("x-amz-server-side-encryption", "AES256") :: Nil

  @InternalApi private[s3] override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
    case PutObject | InitiateMultipartUpload => headers
    case _ => Nil
  }

  override def toString =
    "ServerSideEncryption.AES256(" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: KMS => true
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash()
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
 *
 * @param keyId amazon resource name in the "arn:aws:kms:my-region:my-account-id:key/my-key-id" format.
 * @param context optional base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs
 */
final class KMS private[headers] (val keyId: String, val context: Option[String]) extends ServerSideEncryption {

  @InternalApi private[s3] override def headers: immutable.Seq[HttpHeader] = {
    val baseHeaders = RawHeader("x-amz-server-side-encryption", "aws:kms") ::
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId) ::
      Nil

    val headers = baseHeaders ++ context.map(ctx => RawHeader("x-amz-server-side-encryption-context", ctx)).toSeq
    headers
  }

  @InternalApi private[s3] override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
    case PutObject | InitiateMultipartUpload => headers
    case _ => Nil
  }

  def withKeyId(keyId: String) = copy(keyId = keyId)
  def withContext(context: String) = copy(context = Some(context))

  private def copy(
      keyId: String = keyId,
      context: Option[String] = context
  ): KMS = new KMS(
    keyId = keyId,
    context = context
  )

  override def toString =
    "ServerSideEncryption.KMS(" +
    s"keyId=$keyId," +
    s"context=$context" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: KMS =>
      Objects.equals(this.keyId, that.keyId) &&
      Objects.equals(this.context, that.context)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(keyId, context)
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
 *
 * @param key base64-encoded encryption key for Amazon S3 to use to encrypt or decrypt your data.
 * @param md5 optional base64-encoded 128-bit MD5 digest of the encryption key according to RFC 1321.
 */
final class CustomerKeys private[headers] (val key: String, val md5: Option[String] = None)
    extends ServerSideEncryption {

  @InternalApi private[s3] override def headers: immutable.Seq[HttpHeader] =
    RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256") ::
    RawHeader("x-amz-server-side-encryption-customer-key", key) ::
    RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5.getOrElse({
      val decodedKey = BinaryUtils.fromBase64(key)
      val md5 = Md5Utils.md5AsBase64(decodedKey)
      md5
    })) :: Nil

  @InternalApi private[s3] override def headersFor(request: S3Request): immutable.Seq[HttpHeader] = request match {
    case GetObject | HeadObject | PutObject | InitiateMultipartUpload | UploadPart =>
      headers
    case CopyPart =>
      val copyHeaders =
        RawHeader("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256") ::
        RawHeader("x-amz-copy-source-server-side-encryption-customer-key", key) ::
        RawHeader(
          "x-amz-copy-source-server-side-encryption-customer-key-MD5",
          md5.getOrElse({
            val decodedKey = BinaryUtils.fromBase64(key)
            val md5 = Md5Utils.md5AsBase64(decodedKey)
            md5
          })
        ) :: Nil
      headers ++: copyHeaders
    case _ => Nil
  }

  def withKey(key: String) = copy(key = key)
  def withMd5(md5: String) = copy(md5 = Some(md5))

  private def copy(
      key: String = key,
      md5: Option[String] = md5
  ): CustomerKeys = new CustomerKeys(
    key = key,
    md5 = md5
  )

  override def toString =
    "ServerSideEncryption.CustomerKeys(" +
    s"key=$key," +
    s"md5=$md5" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: CustomerKeys =>
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.md5, that.md5)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(key, md5)
}
