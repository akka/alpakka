/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import java.time.{Duration => JavaDuration}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

final class CassandraBatchSettings private (val maxGroupSize: Int, val maxGroupWait: FiniteDuration) {

  def withMaxGroupSize(maxGroupSize: Int): CassandraBatchSettings =
    copy(maxGroupSize = maxGroupSize)

  def withMaxGroupWait(maxGroupWait: FiniteDuration): CassandraBatchSettings =
    copy(maxGroupWait = maxGroupWait)

  def withMaxGroupWait(maxGroupWait: JavaDuration): CassandraBatchSettings =
    copy(maxGroupWait = Duration.fromNanos(maxGroupWait.toNanos))

  private def copy(maxGroupSize: Int = maxGroupSize, maxGroupWait: FiniteDuration = maxGroupWait) =
    new CassandraBatchSettings(maxGroupSize, maxGroupWait)

  override def toString: String =
    s"CassandraBatchSettings(maxGroupSize=$maxGroupSize, maxGroupWait=$maxGroupWait)"
}

object CassandraBatchSettings {

  def apply(maxGroupSize: Int, maxGroupWait: FiniteDuration): CassandraBatchSettings = {
    require(
      maxGroupSize > 0,
      s"Invalid value for maxGroupSize: $maxGroupSize. It should be > 0."
    )

    new CassandraBatchSettings(maxGroupSize, maxGroupWait)
  }

  /**
   * Java API
   */
  def create(maxGroupSize: Int, maxGroupWait: FiniteDuration): CassandraBatchSettings =
    CassandraBatchSettings(maxGroupSize, maxGroupWait)

  def apply(): CassandraBatchSettings = CassandraBatchSettings(50, 500.millis)

  /**
   * Java API
   */
  def create(): CassandraBatchSettings = CassandraBatchSettings()
}
