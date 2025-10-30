/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.mock

import java.io.ByteArrayOutputStream
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.protobuf.ByteString
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.avro
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory

import java.util.Base64
import scala.util.Random

trait BigQueryMockData {

  val Project = "mock-proj"
  val ProjectFullName = s"projects/$Project"
  val Location = "mock-location"
  val Dataset = "mock-dataset"
  val Table = "mock-table"
  val TableFullName = s"projects/$Project/datasets/$Dataset/tables/$Table"
  def readSessionName() = s"projects/$Project/locations/$Location/sessions/mock-session-${Random.nextLong()}"

  val Col1 = "col1"
  val Col2 = "col2"

  val GCPSerializedArrowSchema: ByteString = ByteString.copyFrom(
    Base64.getDecoder.decode(
      "/////7AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABQAAAABAAAAMj///8AAAECEAAAACAAAAAEAAAAAAAAAAQAAABjb2wyAAAAAAgADAAIAAcACAAAAAAAAAFAAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAABwAAAAEAAAAAAAAAAQAAABjb2wxAAAAAAQABAAEAAAAAAAAAA=="
    )
  )

  val GCPSerializedArrowTenRecordBatch: ByteString = ByteString.copyFrom(
    Base64.getDecoder.decode(
      "/////8gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAABgAAAAAAAAAAAAKABgADAAEAAgACgAAAGwAAAAQAAAAAQAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAEAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAgAAAAAAAAAAAAAAAIAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAdmFsMQAAAAACAAAAAAAAAA=="
    )
  )

  val FullArrowSchema: Schema = Schema.fromJSON("""
      |{
      |  "fields" : [ {
      |    "name" : "col1",
      |    "nullable" : true,
      |    "type" : {
      |      "name" : "utf8"
      |    },
      |    "children" : [ ]
      |  }, {
      |    "name" : "col2",
      |    "nullable" : true,
      |    "type" : {
      |      "name" : "int",
      |      "bitWidth" : 64,
      |      "isSigned" : true
      |    },
      |    "children" : [ ]
      |  } ]
      |}
      |""".stripMargin)

  val FullAvroSchema: avro.Schema =
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

  val Col1Schema: avro.Schema =
    new avro.Schema.Parser().parse("""
                                     |{
                                     |  "type": "record",
                                     |  "name": "TestRecord",
                                     |  "fields": [
                                     |    {"name": "col1", "type": "string" }
                                     |  ]
                                     |}
      """.stripMargin)

  val Col2Schema: avro.Schema =
    new avro.Schema.Parser().parse("""
                                     |{
                                     |  "type": "record",
                                     |  "name": "TestRecord",
                                     |  "fields": [
                                     |    {"name": "col2", "type": "int" }
                                     |  ]
                                     |}
      """.stripMargin)

  val DefaultNumStreams = 10
  val ResponsesPerStream = 10
  val RecordsPerReadRowsResponse = 10
  val TotalRecords = DefaultNumStreams * ResponsesPerStream * RecordsPerReadRowsResponse

  val FullAvroRecord = new GenericRecordBuilder(FullAvroSchema).set("col1", "val1").set("col2", 2).build()
  val Col1AvroRecord = new GenericRecordBuilder(Col1Schema).set("col1", "val1").build()
  val Col2AvroRecord = new GenericRecordBuilder(Col2Schema).set("col2", 2).build()

  def recordsAsRows(record: GenericRecord): AvroRows = {
    val datumWriter = new GenericDatumWriter[GenericRecord](record.getSchema)

    val outputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(outputStream, null)

    List.fill(RecordsPerReadRowsResponse)(record).foreach(datumWriter.write(_, encoder))

    encoder.flush()

    AvroRows(ByteString.copyFrom(outputStream.toByteArray))
  }
}
