/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.util.Objects

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.headers.{CannedAcl, StorageClass}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.collection.JavaConverters._

final class MetaHeaders private (val metaHeaders: Map[String, String]) {
  def headers: immutable.Seq[HttpHeader] =
    metaHeaders.map { header =>
      RawHeader(s"x-amz-meta-${header._1}", header._2)
    }(collection.breakOut): immutable.Seq[HttpHeader]

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
 * Container for headers used in s3 uploads like acl, server side encryption, storage class,
 * metadata or custom headers for more advanced use cases.
 */
final class S3Headers private (val cannedAcl: Option[CannedAcl] = None,
                               val metaHeaders: Option[MetaHeaders] = None,
                               val storageClass: Option[StorageClass] = None,
                               val customHeaders: Map[String, String] = Map.empty) {

  val headers: Seq[HttpHeader] =
  cannedAcl.to[Seq].map(_.header) ++
  metaHeaders.to[Seq].flatMap(_.headers) ++
  storageClass.to[Seq].map(_.header) ++
  customHeaders.map { header =>
    RawHeader(header._1, header._2)
  }

  def withCannedAcl(cannedAcl: CannedAcl) = copy(cannedAcl = Some(cannedAcl))
  def withMetaHeaders(metaHeaders: MetaHeaders) = copy(metaHeaders = Some(metaHeaders))
  def withStorageClass(storageClass: StorageClass) = copy(storageClass = Some(storageClass))
  def withCustomHeaders(customHeaders: Map[String, String]) = copy(customHeaders = customHeaders)

  private def copy(
      cannedAcl: Option[CannedAcl] = cannedAcl,
      metaHeaders: Option[MetaHeaders] = metaHeaders,
      storageClass: Option[StorageClass] = storageClass,
      customHeaders: Map[String, String] = customHeaders
  ): S3Headers = new S3Headers(
    cannedAcl = cannedAcl,
    metaHeaders = metaHeaders,
    storageClass = storageClass,
    customHeaders = customHeaders
  )

  override def toString =
    "S3Headers(" +
    s"cannedAcl=$cannedAcl," +
    s"metaHeaders=$metaHeaders," +
    s"storageClass=$storageClass," +
    s"customHeaders=$customHeaders," +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: S3Headers =>
      Objects.equals(this.cannedAcl, that.cannedAcl) &&
      Objects.equals(this.metaHeaders, that.metaHeaders) &&
      Objects.equals(this.storageClass, that.storageClass) &&
      Objects.equals(this.customHeaders, that.customHeaders)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(cannedAcl, metaHeaders, storageClass, customHeaders)
}

/**
 * Convenience apply methods for creation of canned acl, meta, storage class or custom headers.
 */
object S3Headers {
  def apply() = new S3Headers()

  /**
   * Java Api
   */
  def create() = new S3Headers()
}
