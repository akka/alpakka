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
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.storage.BigQueryReadClient
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

/**
 * Google BigQuery Storage Api Akka Stream operator factory using Avro Format.
 */
object BigQueryAvroStorage {

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String,
                        readOptions: Option[TableReadOptions] = None,
                        maxNumStreams: Int = 0): Source[Seq[BigQueryRecord], Future[NotUsed]] =
    readAndMapTo(projectId,
                 datasetId,
                 tableId,
                 readOptions,
                 maxNumStreams,
                 (_, client, session) => AvroSource.readRecordsMerged(client, session))
      .flatMapConcat(a => a)

  def readRecords(projectId: String,
                  datasetId: String,
                  tableId: String,
                  readOptions: Option[TableReadOptions] = None,
                  maxNumStreams: Int = 0): Source[Seq[Source[BigQueryRecord, NotUsed]], Future[NotUsed]] =
    readAndMapTo(projectId,
                 datasetId,
                 tableId,
                 readOptions,
                 maxNumStreams,
                 (_, client, session) => AvroSource.readRecords(client, session))

  def readMerged(projectId: String,
                 datasetId: String,
                 tableId: String,
                 readOptions: Option[TableReadOptions] = None,
                 maxNumStreams: Int = 0): Source[(AvroSchema, Source[AvroRows, NotUsed]), Future[NotUsed]] =
    readAndMapTo(projectId,
                 datasetId,
                 tableId,
                 readOptions,
                 maxNumStreams,
                 (schema, client, session) => (schema, AvroSource.readMerged(client, session)))

  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: Option[TableReadOptions] = None,
           maxNumStreams: Int = 0): Source[(AvroSchema, Seq[Source[AvroRows, NotUsed]]), Future[NotUsed]] =
    readAndMapTo(projectId,
                 datasetId,
                 tableId,
                 readOptions,
                 maxNumStreams,
                 (schema, client, session) => (schema, AvroSource.read(client, session)))

  private def readAndMapTo[T](projectId: String,
                              datasetId: String,
                              tableId: String,
                              readOptions: Option[TableReadOptions],
                              maxNumStreams: Int,
                              fx: (AvroSchema, BigQueryReadClient, ReadSession) => T): Source[T, Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
        .map { session =>
          session.schema match {
            case ReadSession.Schema.AvroSchema(schema) => fx(schema, client, session)
            case other => throw new IllegalArgumentException(s"Only Arrow format is supported, received: $other")
          }
        }
    }

}
