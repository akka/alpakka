/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.{ArrowSource, AvroDecoder, AvroSource, SDKClientSource}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.Attributes
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import com.google.cloud.bigquery.storage.v1.arrow.ArrowRecordBatch
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.{
  BigQueryReadClient,
  CreateReadSessionRequest,
  ReadRowsRequest,
  ReadRowsResponse
}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{Future, Promise}

/**
 * Google BigQuery Storage Api Akka Stream operator factory.
 */
object BigQueryStorage {

  private val RequestParamsHeader = "x-goog-request-params"

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

  def readArrow(projectId: String,
                datasetId: String,
                tableId: String,
                readOptions: Option[TableReadOptions] = None,
                maxNumStreams: Int = 0): Source[(ReadSession.Schema, ArrowRecordBatch), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
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
      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
        .map(session => {
          session.schema match {
            case ReadSession.Schema.AvroSchema(_) => AvroSource.readRecords(client, session)
            case ReadSession.Schema.ArrowSchema(_) => ArrowSource.readRecords(client, session)
            case other => throw new IllegalArgumentException(s"Avro, Arrow formats are supported, received: $other")
          }
        })
        .flatMapConcat(a => a)
  }

  def readRaw(projectId: String,
              datasetId: String,
              tableId: String,
              readOptions: Option[TableReadOptions] = None,
              maxNumStreams: Int = 0): Source[(ReadSession.Schema, ReadRowsResponse.Rows), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
        .map(session => SDKClientSource.read(client, session).map((session.schema, _)))
        .flatMapConcat(a => a)
    }

  /**
   * Create a source that contains a number of sources, one for each stream, or section of the table data.
   * These sources will emit one GenericRecord for each row within that stream.
   *
   * @param projectId the projectId the table is located in
   * @param datasetId the datasetId the table is located in
   * @param tableId the table to query
   * @param readOptions Optional TableReadOptions to reduce the amount of data to return, either by column projection or filtering.
   *                    Without this, the whole table will be streamed
   * @param maxNumStreams An optional max initial number of streams. If unset or zero, the server will provide a value of streams so as to produce reasonable throughput.
   *                      Must be non-negative. The number of streams may be lower than the requested number, depending on the amount parallelism that is reasonable for the table.
   *                      Error will be returned if the max count is greater than the current system max limit of 1,000.
   */
  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: Option[TableReadOptions] = None,
           maxNumStreams: Int = 0): Source[Source[GenericRecord, NotUsed], Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      val schemaS = Promise[String]
      val decoderFlow: Flow[AvroRows, List[GenericRecord], Future[Option[NotUsed]]] = Flow.lazyInitAsync(
        () =>
          schemaS.future.map { ss =>
            val avro = AvroDecoder(ss)
            Flow.fromFunction((r: AvroRows) => avro.decodeRows(r.serializedBinaryRows))
          }(ExecutionContexts.parasitic)
      )

      readSession(client, projectId, datasetId, tableId, readOptions, maxNumStreams)
        .mapConcat { session =>
          session.schema match {
            case ReadSession.Schema.AvroSchema(avroSchema) => schemaS.success(avroSchema.schema)
            case other =>
              schemaS.failure(new IllegalArgumentException(s"Only AvroSchemas are allowed, received: $other"))
          }
          session.streams.toList
        }
        .map(
          stream =>
            client
              .readRows()
              .addHeader(RequestParamsHeader, s"read_stream=${stream.name}")
              .invoke(ReadRowsRequest(stream.name))
              .mapConcat(_.rows.avroRows.toList)
              .via(decoderFlow)
              .mapConcat(identity)
        )
    }

  private def readSession(client: BigQueryReadClient,
                          projectId: String,
                          datasetId: String,
                          tableId: String,
                          readOptions: Option[TableReadOptions] = None,
                          maxNumStreams: Int = 0) =
    Source
      .future {
        val table = s"projects/$projectId/datasets/$datasetId/tables/$tableId"
        client
          .createReadSession()
          .addHeader(RequestParamsHeader, s"read_session.table=$table")
          .invoke(
            CreateReadSessionRequest(
              parent = s"projects/$projectId",
              Some(ReadSession(dataFormat = DataFormat.AVRO, table = table, readOptions = readOptions)),
              maxNumStreams
            )
          )
      }

  private def reader(system: ClassicActorSystemProvider, attr: Attributes) =
    attr
      .get[BigQueryStorageAttributes.BigQueryStorageReader]
      .map(_.client)
      .getOrElse(GrpcBigQueryStorageReaderExt()(system).reader)
}
