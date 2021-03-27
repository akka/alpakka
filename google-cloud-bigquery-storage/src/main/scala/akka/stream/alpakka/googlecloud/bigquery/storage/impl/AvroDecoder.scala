/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryRecord, BigQueryRecordMapImpl}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.Schema.AvroSchema
import com.google.protobuf.ByteString
import org.apache.avro.Schema
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.collection.mutable

/**
 * Internal API
 */
@InternalApi private[bigquery] class AvroDecoder(schema: Schema) {
  val datumReader = new GenericDatumReader[GenericRecord](schema)

  def decodeToRecord(avroRows: ByteString): List[BigQueryRecord] = {
    val result = new mutable.ListBuffer[BigQueryRecord]

    val inputStream = new SeekableByteArrayInput(avroRows.toByteArray)
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    while (!decoder.isEnd) {
      val item = datumReader.read(null, decoder)

      result += item
    }

    result.toList
  }



  def decodeRows(avroRows: ByteString): List[GenericRecord] = {
    val result = new mutable.ListBuffer[GenericRecord]

    val inputStream = new SeekableByteArrayInput(avroRows.toByteArray)
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    while (!decoder.isEnd) {
      val item = datumReader.read(null, decoder)

      result += item
    }

    result.toList
  }
}

@InternalApi private[bigquery] object AvroDecoder {
  def apply(schema: String): AvroDecoder = new AvroDecoder(new Schema.Parser().parse(schema))

}
