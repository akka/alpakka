/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.scaladsl

import akka.Done
import akka.stream.alpakka.debezium.CommittableRecord
import akka.stream.scaladsl.{Keep, Sink}

import scala.concurrent.Future

object DebeziumSink {
  def committer(): Sink[CommittableRecord, Future[Done]] = {
    DebeziumFlow
      .committer()
      .toMat(Sink.ignore)(Keep.right)
  }
}
