/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium

import akka.Done
import akka.actor.ActorRef
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.Converter

final class CommittableRecord(val record: SourceRecord,
                              private[debezium] val committer: ActorRef,
                              private[debezium] val converter: Converter)
    extends DebeziumRecord {

  private[debezium] def markAsProcessed(): Done = {
    committer ! record
    Done
  }

  override def toString = s"CommittableRecord($record)"
}
