/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

final class FailedUpload private (
    val reasons: Seq[Throwable]
) extends Exception(reasons.map(_.getMessage).mkString(", ")) {

  /** Java API */
  def getReasons: java.util.List[Throwable] = reasons.asJava
}

object FailedUpload {

  def apply(reasons: Seq[Throwable]) = new FailedUpload(reasons)

  /** Java API */
  def create(reasons: java.util.List[Throwable]) = FailedUpload(reasons.asScala.toList)
}
