/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object CassandraBatchSettings {
  val Defaults = CassandraBatchSettings(50, 500.millis)
}

final case class CassandraBatchSettings(maxGroupSize: Int, maxGroupWait: FiniteDuration) {
  require(
    maxGroupSize > 0,
    s"Invalid value for maxGroupSize: $maxGroupSize. It should be > 0."
  )

  def withMaxGroupSize(maxGroupSize: Int): CassandraBatchSettings =
    copy(maxGroupSize = maxGroupSize)

  def withMaxGroupWait(maxGroupWait: Long, timeUnit: TimeUnit): CassandraBatchSettings =
    copy(maxGroupWait = Duration(maxGroupWait, timeUnit))
}
