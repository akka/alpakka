/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.annotation.InternalApi

import scala.compat.java8.OptionConverters._

/**
 * Stream element type emitted by Elasticsearch sources.
 *
 * The constructor is INTERNAL API, but you may construct instances for testing by using
 * [[akka.stream.alpakka.elasticsearch.testkit.MessageFactory]].
 */
final class ReadResult[T] @InternalApi private[elasticsearch] (val id: String,
                                                               val source: T,
                                                               val version: Option[Long]) {

  /** Java API */
  def getVersion: java.util.Optional[Long] = version.asJava

  override def toString =
    s"""ReadResult(id=$id,source=$source,version=${version.getOrElse("")})"""

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
