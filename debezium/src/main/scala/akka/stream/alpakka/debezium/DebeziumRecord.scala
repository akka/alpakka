/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium

import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.Converter

private[debezium] trait DebeziumRecord {
  private[debezium] val converter: Converter

  val record: SourceRecord

  lazy val payload: Array[Byte] = {
    val schemaBuilder = SchemaBuilder.struct.field("key", record.keySchema)
    if (record.valueSchema != null) schemaBuilder.field("value", record.valueSchema)

    val schema = schemaBuilder.build

    val message = new Struct(schema)
    message.put("key", record.key)

    if (record.value != null) message.put("value", record.value)

    converter.fromConnectData("", schema, message)
  }
}
