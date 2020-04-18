/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.impl

import akka.actor.Actor
import io.debezium.engine.DebeziumEngine.RecordCommitter
import org.apache.kafka.connect.source.SourceRecord

import scala.concurrent.{Future, Promise}

private[debezium] class CommitterActor(records: List[SourceRecord],
                                       promise: Promise[Unit],
                                       committer: RecordCommitter[SourceRecord])
    extends Actor {
  var processed: Set[SourceRecord] = Set.empty

  override def receive: Receive = {
    case record: SourceRecord =>
      processed = processed + record
      committer.markProcessed(record)

      if (processed.size == records.size) {
        committer.markBatchFinished()
        promise.completeWith(Future.unit)
        context.stop(self)
      }
  }
}
