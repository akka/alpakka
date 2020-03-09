/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.mock

import java.io.ByteArrayOutputStream

import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.protobuf.ByteString
import org.apache.avro
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory

trait BigQueryMockData {

  val Project = "mock-proj"
  val ProjectFullName = s"projects/$Project"
  val Location = "mock-location"
  val Dataset = "mock-dataset"
  val Table = "mock-table"
  val TableFullName = s"projects/$Project/datasets/$Dataset/tables/$Table"
  val ReadSessionName = s"projects/$Project/locations/$Location/sessions/mock-session"

  val Schema: avro.Schema =
    new avro.Schema.Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "TestRecord",
      |  "fields": [
      |    {"name": "col1", "type": "string" },
      |    {"name": "col2", "type": "int" }
      |  ]
      |}
    """.stripMargin)

  val DefaultNumStreams = 10
  val ResponsesPerStream = 10
  val RecordsPerReadRowsResponse = 10
  val Record = new GenericRecordBuilder(Schema).set("col1", "val1").set("col2", 2).build()

  val RecordsAsRows: AvroRows = {
    val datumWriter = new GenericDatumWriter[GenericRecord](Schema)

    val outputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(outputStream, null)

    List.fill(RecordsPerReadRowsResponse)(Record).foreach(datumWriter.write(_, encoder))

    encoder.flush()

    AvroRows(ByteString.copyFrom(outputStream.toByteArray))
  }
}
