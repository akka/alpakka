/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.{ArrowSource, AvroSource}
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage.{readSession, reader}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

object BigQueryAvroStorage {

  def readAvro(projectId: String,
               datasetId: String,
               tableId: String,
               readOptions: Option[TableReadOptions] = None,
               maxNumStreams: Int = 0): Source[(ReadSession.Schema, AvroRows), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
        .map(session => {
          session.schema match {
            case ReadSession.Schema.AvroSchema(_) => AvroSource.read(client, session)
            case other => throw new IllegalArgumentException(s"Only Avro format is supported, received: $other")
          }
        })
        .flatMapConcat(a => a)
    }

  def readRecords(projectId: String,
                  datasetId: String,
                  tableId: String,
                  readOptions: Option[TableReadOptions] = None,
                  maxNumStreams: Int = 0): Source[List[BigQueryRecord], Future[NotUsed]] = Source.fromMaterializer {
    (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
        .map(session => {
          session.schema match {
            case ReadSession.Schema.AvroSchema(_) => AvroSource.readRecords(client, session)
            case other => throw new IllegalArgumentException(s"Avro, Arrow formats are supported, received: $other")
          }
        })
        .flatMapConcat(a => a)
  }

}
