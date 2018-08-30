/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.annotation.{ApiMayChange, InternalApi}

import scala.compat.java8.OptionConverters._

final class ReadResult[T] private (val id: String, val source: T, val version: Option[Long]) {

  /** Java API */
  def getId: String = id

  /** Java API */
  def getSource: T = source

  /** Java API */
  def getVersion: java.util.Optional[Long] = version.asJava

  override def toString =
    s"""ReadResult(id=$id,source=$source,version=$version)"""

  override def equals(other: Any): Boolean = other match {
    case that: ReadResult[_] =>
      java.util.Objects.equals(this.id, that.id) &&
      java.util.Objects.equals(this.source, that.source) &&
      java.util.Objects.equals(this.version, that.version)
    case _ => false
  }

  override def hashCode(): Int =
    source match {
      case o: AnyRef =>
        java.util.Objects.hash(id, o, version)
      case _ =>
        // TODO include source: AnyVal in hashcode
        java.util.Objects.hash(id, version)
    }
}

object ReadResult {

  /**
   * Scala API
   * For use with testing.
   */
  @ApiMayChange
  def apply[T](
      id: String,
      source: T,
      version: Option[Long]
  ): ReadResult[T] = new ReadResult(
    id,
    source,
    version
  )

  /**
   * Java API
   * For use with testing.
   */
  @ApiMayChange
  def create[T](
      id: String,
      source: T,
      version: java.util.Optional[Long]
  ): ReadResult[T] = new ReadResult(
    id,
    source,
    version.asScala
  )
}
