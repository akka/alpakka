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
case class S3Headers(headers: Seq[HttpHeader])

case class MetaHeaders(metaHeaders: Map[String, String]) {
  def headers =
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
