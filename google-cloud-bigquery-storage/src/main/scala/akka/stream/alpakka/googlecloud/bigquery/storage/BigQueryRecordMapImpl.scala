package akka.stream.alpakka.googlecloud.bigquery.storage

import org.apache.avro.generic.GenericRecord

trait BigQueryRecord {

  def get(column: String): Option[Object]

}

object BigQueryRecord {

  def fromMap(map: Map[String,Object]): BigQueryRecord = new BigQueryRecordMapImpl(map)

  def fromAvro(record: GenericRecord): BigQueryRecord = new BigQueryRecordAvroImpl(record)

}

class BigQueryRecordAvroImpl(record: GenericRecord) extends BigQueryRecord {

  override def get(column: String): Option[Object] = Option(record.get(column))

}

class BigQueryRecordMapImpl(map: Map[String,Object]) extends BigQueryRecord {

  override def get(column: String): Option[Object] = map.get(column)

}
