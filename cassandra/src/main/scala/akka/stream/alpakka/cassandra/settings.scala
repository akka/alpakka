/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.util.JavaDurationConverters._

final class CassandraBatchSettings private (val maxGroupSize: Int, val maxGroupWait: FiniteDuration) {
  require(
    maxGroupSize > 0,
    s"Invalid value for maxGroupSize: $maxGroupSize. It should be > 0."
  )

  def withMaxGroupSize(maxGroupSize: Int): CassandraBatchSettings =
    copy(maxGroupSize = maxGroupSize)

  def withMaxGroupWait(maxGroupWait: FiniteDuration): CassandraBatchSettings =
    copy(maxGroupWait = maxGroupWait)

  def withMaxGroupWait(maxGroupWait: java.time.Duration): CassandraBatchSettings =
    copy(maxGroupWait = maxGroupWait.asScala)

  private def copy(maxGroupSize: Int = maxGroupSize, maxGroupWait: FiniteDuration = maxGroupWait) =
    new CassandraBatchSettings(maxGroupSize, maxGroupWait)

  override def toString: String =
    s"CassandraBatchSettings(maxGroupSize=$maxGroupSize, maxGroupWait=$maxGroupWait)"
}

object CassandraBatchSettings {

  def apply(maxGroupSize: Int, maxGroupWait: FiniteDuration): CassandraBatchSettings =
    new CassandraBatchSettings(maxGroupSize, maxGroupWait)

  /**
   * Java API
   */
  def create(maxGroupSize: Int, maxGroupWait: java.time.Duration): CassandraBatchSettings =
    CassandraBatchSettings(maxGroupSize, maxGroupWait.asScala)

  val default = new CassandraBatchSettings(50, 500.millis)

  def apply(): CassandraBatchSettings = default

  /**
   * Java API
   */
  def create(): CassandraBatchSettings = default
}
