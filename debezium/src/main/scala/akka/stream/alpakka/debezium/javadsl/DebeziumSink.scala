/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.javadsl

import akka.Done
import akka.stream.alpakka.debezium.CommittableRecord
import akka.stream.alpakka.debezium.scaladsl.{DebeziumSink => ScalaDebeziumSink}
import akka.stream.javadsl._

import scala.concurrent.Future

object DebeziumSink {
  def committer(): Sink[CommittableRecord, Future[Done]] =
    ScalaDebeziumSink
      .committer()
      .asJava
}
