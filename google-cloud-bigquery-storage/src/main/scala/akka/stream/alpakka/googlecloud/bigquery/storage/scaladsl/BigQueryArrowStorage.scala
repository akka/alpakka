/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.ArrowSource
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage.{readSession, reader}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

object BigQueryArrowStorage {

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String,
                        readOptions: Option[TableReadOptions] = None,
                        maxNumStreams: Int = 0): Source[Seq[BigQueryRecord], Future[NotUsed]] =
    Source
      .fromMaterializer { (mat, attr) =>
        val client = reader(mat.system, attr).client
        readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
          .map { session =>
            session.schema match {
              case ReadSession.Schema.ArrowSchema(_) => ArrowSource.readRecordsMerged(client, session)
              case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
            }
          }
      }
      .flatMapConcat(a => a)

  def readRecords(projectId: String,
                  datasetId: String,
                  tableId: String,
                  readOptions: Option[TableReadOptions] = None,
                  maxNumStreams: Int = 0): Source[Seq[Source[BigQueryRecord, NotUsed]], Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.ArrowSchema(_) => ArrowSource.readRecords(client, session)
            case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
          }
        }
    }

  def readMerged(projectId: String,
                 datasetId: String,
                 tableId: String,
                 readOptions: Option[TableReadOptions] = None,
                 maxNumStreams: Int = 0): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.ArrowSchema(schema) => (schema, ArrowSource.readMerged(client, session))
            case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
          }
        }
    }

  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: Option[TableReadOptions] = None,
           maxNumStreams: Int = 0): Source[(ArrowSchema, Seq[Source[ArrowRecordBatch, NotUsed]]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.ARROW, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.ArrowSchema(schema) => (schema, ArrowSource.read(client, session))
            case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
          }
        }
    }

}
