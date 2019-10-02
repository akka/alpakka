/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.util.Objects

import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption, StorageClass}
import akka.stream.alpakka.s3.impl.S3Request

import scala.collection.immutable.Seq
import scala.collection.JavaConverters._

final class MetaHeaders private (val metaHeaders: Map[String, String]) {

  @InternalApi private[s3] def headers: Seq[HttpHeader] =
    metaHeaders.toIndexedSeq.map { header =>
      RawHeader(s"x-amz-meta-${header._1}", header._2)
    }

  def withMetaHeaders(metaHeaders: Map[String, String]) = new MetaHeaders(
    metaHeaders = metaHeaders
  )

  override def toString =
    "MetaHeaders(" +
    s"metaHeaders=$metaHeaders," +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: S3Headers =>
      Objects.equals(this.metaHeaders, that.metaHeaders)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(metaHeaders)
}

object MetaHeaders {
  def apply(metaHeaders: Map[String, String]) =
    new MetaHeaders(metaHeaders)

  def create(metaHeaders: java.util.Map[String, String]) =
    apply(metaHeaders.asScala.toMap)
}

/**
 * Container for headers used in s3 uploads like acl, storage class,
 * metadata, server side encryption or custom headers for more advanced use cases.
 */
final class S3Headers private (val cannedAcl: Option[CannedAcl] = None,
                               val metaHeaders: Option[MetaHeaders] = None,
                               val storageClass: Option[StorageClass] = None,
                               val customHeaders: Map[String, String] = Map.empty,
                               val serverSideEncryption: Option[ServerSideEncryption] = None) {

  @InternalApi private[s3] val headers: Seq[HttpHeader] =
    cannedAcl.toIndexedSeq.map(_.header) ++
    metaHeaders.toIndexedSeq.flatMap(_.headers) ++
    storageClass.toIndexedSeq.map(_.header) ++
    customHeaders.map { header =>
      RawHeader(header._1, header._2)
    }

  @InternalApi private[s3] def headersFor(request: S3Request) =
    headers ++ serverSideEncryption.toIndexedSeq.flatMap(_.headersFor(request))

  def withCannedAcl(cannedAcl: CannedAcl) = copy(cannedAcl = Some(cannedAcl))
  def withMetaHeaders(metaHeaders: MetaHeaders) = copy(metaHeaders = Some(metaHeaders))
  def withStorageClass(storageClass: StorageClass) = copy(storageClass = Some(storageClass))
  def withCustomHeaders(customHeaders: Map[String, String]) = copy(customHeaders = customHeaders)
  def withServerSideEncryption(serverSideEncryption: ServerSideEncryption) =
    copy(serverSideEncryption = Some(serverSideEncryption))
  def withOptionalServerSideEncryption(serverSideEncryption: Option[ServerSideEncryption]) =
    copy(serverSideEncryption = serverSideEncryption)

  private def copy(
      cannedAcl: Option[CannedAcl] = cannedAcl,
      metaHeaders: Option[MetaHeaders] = metaHeaders,
      storageClass: Option[StorageClass] = storageClass,
      customHeaders: Map[String, String] = customHeaders,
      serverSideEncryption: Option[ServerSideEncryption] = serverSideEncryption
  ): S3Headers = new S3Headers(
    cannedAcl = cannedAcl,
    metaHeaders = metaHeaders,
    storageClass = storageClass,
    customHeaders = customHeaders,
    serverSideEncryption = serverSideEncryption
  )

  override def toString =
    "S3Headers(" +
    s"cannedAcl=$cannedAcl," +
    s"metaHeaders=$metaHeaders," +
    s"storageClass=$storageClass," +
    s"customHeaders=$customHeaders," +
    s"serverSideEncryption=$serverSideEncryption" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: S3Headers =>
      Objects.equals(this.cannedAcl, that.cannedAcl) &&
      Objects.equals(this.metaHeaders, that.metaHeaders) &&
      Objects.equals(this.storageClass, that.storageClass) &&
      Objects.equals(this.customHeaders, that.customHeaders) &&
      Objects.equals(this.serverSideEncryption, that.serverSideEncryption)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(cannedAcl, metaHeaders, storageClass, customHeaders, serverSideEncryption)
}

/**
 * Convenience apply methods for creation of canned acl, meta, storage class or custom headers.
 */
object S3Headers {

  /**
   * Empty set of headers
   */
  val empty: S3Headers = new S3Headers()

  def apply() = empty

  /**
   * Java Api
   */
  def create() = empty
}
