/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import org.apache.avro.generic.GenericRecord

trait BigQueryRecord {

  def get(column: String): Option[Object]

}

object BigQueryRecord {

  def fromMap(map: Map[String, Object]): BigQueryRecord = new BigQueryRecordMapImpl(map)

  def fromAvro(record: GenericRecord): BigQueryRecord = new BigQueryRecordAvroImpl(record)

}

case class BigQueryRecordAvroImpl(record: GenericRecord) extends BigQueryRecord {

  override def get(column: String): Option[Object] = Option(record.get(column))

  override def equals(that: Any): Boolean = that match {
    case BigQueryRecordAvroImpl(thatRecord) => thatRecord.equals(record)
    case _ => false
  }

  override def hashCode(): Int = record.hashCode()

}

case class BigQueryRecordMapImpl(map: Map[String, Object]) extends BigQueryRecord {

  override def get(column: String): Option[Object] = map.get(column)

  override def equals(that: Any): Boolean = that match {
    case BigQueryRecordMapImpl(thatMap) => thatMap.equals(map)
    case _ => false
  }

  override def hashCode(): Int = map.hashCode()

}
