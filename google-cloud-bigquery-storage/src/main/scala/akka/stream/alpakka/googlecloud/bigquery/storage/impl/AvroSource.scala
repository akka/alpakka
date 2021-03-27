package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryRecord, BigQueryRecordMapImpl}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.BigQueryReadClient
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import com.google.cloud.bigquery.storage.v1.storage.ReadRowsRequest

object AvroSource {

  private val RequestParamsHeader = "x-goog-request-params"

  def readRecords(client: BigQueryReadClient, readSession: ReadSession): Source[List[BigQueryRecord], NotUsed] =
    client.readRows()
      .addHeader(RequestParamsHeader, s"read_stream=${readSession.name}")
      .invoke(ReadRowsRequest(readSession.name))
      .mapConcat(_.rows.avroRows.toList)
      .map(a => AvroDecoder(readSession.schema.avroSchema.get.schema).decodeRows(a.serializedBinaryRows))
      .map(_.map(BigQueryRecord.fromAvro))

  def read(client: BigQueryReadClient, readSession: ReadSession): Source[(ReadSession.Schema, AvroRows), NotUsed]=
    client.readRows()
      .addHeader(RequestParamsHeader, s"read_stream=${readSession.name}")
      .invoke(ReadRowsRequest(readSession.name))
      .mapConcat(_.rows.avroRows.toList)
      .map((readSession.schema, _))

}
