/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.acl.CannedAcl

import scala.collection.immutable

/**
 * Container for headers used in s3 uploads like acl, server side encryption, storage class,
 * metadata or custom headers for more advanced use cases.
 */
case class S3Headers(cannedAcl: CannedAcl = CannedAcl.Private,
                     metaHeaders: MetaHeaders = MetaHeaders(Map()),
                     customHeaders: Option[AmzHeaders] = None) {

  def headers: immutable.Seq[HttpHeader] =
    metaHeaders.headers ++ customHeaders.map(_.headers).getOrElse(Nil) :+ cannedAcl.header
}

case class MetaHeaders(metaHeaders: Map[String, String]) {
  def headers =
    metaHeaders.map { header =>
      RawHeader(s"x-amz-meta-${header._1}", header._2)
    }(collection.breakOut): immutable.Seq[HttpHeader]
}

case class AmzHeaders(headers: Seq[HttpHeader])

/**
 * Convenience apply methods for creation of encryption, storage class or custom headers.
 */
object AmzHeaders {
  def apply(storageClass: StorageClass): AmzHeaders = new AmzHeaders(Seq(storageClass.header))
  def apply(encryption: ServerSideEncryption): AmzHeaders = new AmzHeaders(encryption.headers)
  def apply(customHeaders: Map[String, String]): AmzHeaders =
    new AmzHeaders(customHeaders.map { header =>
      RawHeader(header._1, header._2)
    }(collection.breakOut))
  def apply(encryption: ServerSideEncryption, storageClass: StorageClass): AmzHeaders =
    new AmzHeaders(encryption.headers :+ storageClass.header)
  def apply(encryption: ServerSideEncryption, customHeaders: Seq[HttpHeader], storageClass: StorageClass): AmzHeaders =
    new AmzHeaders(encryption.headers ++ customHeaders :+ storageClass.header)
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html
 */
sealed abstract class StorageClass(storageClass: String) {
  def header: HttpHeader = RawHeader("x-amz-storage-class", storageClass)
}

object StorageClass {
  case object Standard extends StorageClass("STANDARD")
  case object InfrequentAccess extends StorageClass("STANDARD_IA")
  case object ReducedRedundancy extends StorageClass("REDUCED_REDUNDANCY")
}

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
 * @param algorithm AES-256 or aws:kms
 * @param kmsKeyId optional amazon resource name in the "arn:aws:kms:my-region:my-account-id:key/my-key-id" format.
 * @param context optional base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs
 */
sealed abstract class ServerSideEncryption(algorithm: String,
                                           kmsKeyId: Option[String] = None,
                                           context: Option[String] = None) {
  def headers: immutable.Seq[HttpHeader] = algorithm match {
    case "AES256" => RawHeader("x-amz-server-side-encryption", "AES256") :: Nil
    case "aws:kms" if kmsKeyId.isDefined && context.isEmpty =>
      RawHeader("x-amz-server-side-encryption", "aws:kms") ::
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", kmsKeyId.get) ::
      Nil
    case "aws:kms" if kmsKeyId.isDefined && context.isDefined =>
      RawHeader("x-amz-server-side-encryption", "aws:kms") ::
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", kmsKeyId.get) ::
      RawHeader("x-amz-server-side-encryption-context", context.get) ::
      Nil
    case _ => throw new IllegalArgumentException("Unsupported encryption algorithm.")
  }
}

object ServerSideEncryption {
  case object AES256 extends ServerSideEncryption("AES256")
  case class KMS(keyId: String, context: Option[String] = None)
      extends ServerSideEncryption("aws:kms", Some(keyId), context)
}
