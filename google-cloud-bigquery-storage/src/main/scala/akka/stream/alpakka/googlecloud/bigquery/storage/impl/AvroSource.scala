package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryRecord, BigQueryRecordMapImpl}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient, ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession

object AvroSource {

  def readRecords(client: BigQueryReadClient, readSession: ReadSession): Source[List[BigQueryRecord], NotUsed] =
    SDKClientSource.read(client, readSession)
      .mapConcat(_.avroRows.toList)
      .map(a => AvroDecoder(readSession.schema.avroSchema.get.schema).decodeRows(a.serializedBinaryRows))
      .map(_.map(BigQueryRecord.fromAvro))

  def read(client: BigQueryReadClient, readSession: ReadSession): Source[(ReadSession.Schema, AvroRows), NotUsed]=
    SDKClientSource.read(client, readSession)
      .mapConcat(_.avroRows.toList)
      .map((readSession.schema, _))

}
