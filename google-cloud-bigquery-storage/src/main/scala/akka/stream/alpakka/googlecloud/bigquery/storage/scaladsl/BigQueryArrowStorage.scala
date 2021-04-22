/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.{ArrowSource, AvroSource}
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage.{readSession, reader}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.arrow.ArrowRecordBatch
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

object BigQueryArrowStorage {

  def readArrow(projectId: String,
                datasetId: String,
                tableId: String,
                readOptions: Option[TableReadOptions] = None,
                maxNumStreams: Int = 0): Source[(ReadSession.Schema, ArrowRecordBatch), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
        .map(session => {
          session.schema match {
            case ReadSession.Schema.ArrowSchema(_) => ArrowSource.read(client, session)
            case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
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
      readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
        .map(session => {
          session.schema match {
            case ReadSession.Schema.ArrowSchema(_) => ArrowSource.readRecords(client, session)
            case other => throw new IllegalArgumentException(s"Avro, Arrow formats are supported, received: $other")
          }
        })
        .flatMapConcat(a => a)
  }

}
