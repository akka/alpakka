/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.BigQueryReadClient
import com.google.cloud.bigquery.storage.v1.stream.ReadSession

object AvroSource {

  def readRecordsMerged(client: BigQueryReadClient, readSession: ReadSession): Source[List[BigQueryRecord], NotUsed] = {
    readMerged(client, readSession)
      .map(a => AvroDecoder(readSession.schema.avroSchema.get.schema).decodeRows(a.serializedBinaryRows))
      .map(_.map(BigQueryRecord.fromAvro))
  }

  def readMerged(client: BigQueryReadClient, session: ReadSession): Source[AvroRows, NotUsed] =
    read(client, session).reduce((a, b) => a.merge(b))

  def readRecords(client: BigQueryReadClient, session: ReadSession): Seq[Source[BigQueryRecord, NotUsed]] =
    read(client, session)
      .map { a =>
        a.map(
            b =>
              AvroDecoder(session.schema.avroSchema.get.schema)
                .decodeRows(b.serializedBinaryRows)
                .map(BigQueryRecord.fromAvro)
          )
          .mapConcat(c => c)
      }

  def read(client: BigQueryReadClient, session: ReadSession): Seq[Source[AvroRows, NotUsed]] =
    SDKClientSource
      .read(client, session)
      .map(
        s =>
          s.map(r => r.avroRows.toList)
            .mapConcat(a => a)
      )

}
