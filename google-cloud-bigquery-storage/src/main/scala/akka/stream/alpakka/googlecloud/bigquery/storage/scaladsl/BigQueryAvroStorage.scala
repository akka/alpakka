/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AvroSource
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage.{readSession, reader}
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.avro.{AvroRows, AvroSchema}
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

object BigQueryAvroStorage {

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String,
                        readOptions: Option[TableReadOptions] = None,
                        maxNumStreams: Int = 0): Source[Seq[BigQueryRecord], Future[NotUsed]] =
    Source
      .fromMaterializer { (mat, attr) =>
        val client = reader(mat.system, attr).client
        readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
          .map { session =>
            session.schema match {
              case ReadSession.Schema.AvroSchema(_) => AvroSource.readRecordsMerged(client, session)
              case other => throw new IllegalArgumentException(s"Only Avro format is supported, received: $other")
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
      readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.AvroSchema(_) => AvroSource.readRecords(client, session)
            case other => throw new IllegalArgumentException(s"Only Avro format is supported, received: $other")
          }
        }
    }

  def readMerged(projectId: String,
                 datasetId: String,
                 tableId: String,
                 readOptions: Option[TableReadOptions] = None,
                 maxNumStreams: Int = 0): Source[(AvroSchema, Source[AvroRows, NotUsed]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.AvroSchema(schema) => (schema, AvroSource.readMerged(client, session))
            case other => throw new IllegalArgumentException(s"Only Avro format is supported, received: $other")
          }
        }
    }

  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: Option[TableReadOptions] = None,
           maxNumStreams: Int = 0): Source[(AvroSchema, Seq[Source[AvroRows, NotUsed]]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.AvroSchema(schema) => (schema, AvroSource.read(client, session))
            case other => throw new IllegalArgumentException(s"Only Avro format is supported, received: $other")
          }
        }
    }

}
