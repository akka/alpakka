/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.scaladsl

import akka.stream.alpakka.debezium.CommittableRecord
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}

object DebeziumFlow {
  def committer(): Flow[CommittableRecord, Done, NotUsed] = {
    Flow[CommittableRecord]
      .map(_.markAsProcessed())
  }
}
