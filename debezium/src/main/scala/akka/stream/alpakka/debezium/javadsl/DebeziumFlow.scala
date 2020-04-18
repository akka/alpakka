/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.javadsl

import akka.stream.alpakka.debezium.CommittableRecord
import akka.stream.alpakka.debezium.scaladsl.{DebeziumFlow => ScalaDebeziumFlow}
import akka.stream.javadsl.Flow
import akka.{Done, NotUsed}

object DebeziumFlow {
  def committer(): Flow[CommittableRecord, Done, NotUsed] =
    ScalaDebeziumFlow
      .committer()
      .asJava
}
