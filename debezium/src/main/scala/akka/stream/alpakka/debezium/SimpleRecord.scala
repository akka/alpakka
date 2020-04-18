/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.Converter

final class SimpleRecord(val record: SourceRecord, private[debezium] val converter: Converter) extends DebeziumRecord {
  override def toString = s"SimpleRecord($record)"
}
