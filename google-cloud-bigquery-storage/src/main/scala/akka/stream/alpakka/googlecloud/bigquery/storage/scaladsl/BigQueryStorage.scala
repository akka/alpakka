/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.{AvroDecoder, SDKClientSource}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient, CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

/**
 * Google BigQuery Storage Api Akka Stream operator factory.
 */
object BigQueryStorage {

  private val RequestParamsHeader = "x-goog-request-params"

  def readMergedStreams(projectId: String,
                        datasetId: String,
                        tableId: String,
                        dataFormat: DataFormat,
                        readOptions: Option[TableReadOptions] = None,
                        maxNumStreams: Int = 0): Source[(ReadSession.Schema, ReadRowsResponse.Rows), Future[NotUsed]] =
    read(projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
      .map(
        s => {
          s._2.reduce((a, b) => a.merge(b)).map((s._1, _))
        }
      )
      .flatMapConcat(a => a)
      .map(a => a)

  def read(
            projectId: String,
            datasetId: String,
            tableId: String,
            dataFormat: DataFormat,
            readOptions: Option[TableReadOptions] = None,
            maxNumStreams: Int = 0
          ): Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
        .map { session =>
          (session.schema, SDKClientSource.read(client, session))
        }
    }

  def readWithType[A](
               projectId: String,
               datasetId: String,
               tableId: String,
               dataFormat: DataFormat,
               readOptions: Option[TableReadOptions] = None,
               maxNumStreams: Int = 0,
               parallelism: Int = 1
             )(implicit um: FromByteStringUnmarshaller[A]): Source[A, Future[NotUsed]] = {
    Source.fromMaterializer {
      (mat, attr) => {
        implicit val materializer: Materializer = mat
        implicit val executionContext: ExecutionContextExecutor = mat.system.dispatcher
        val client = reader(mat.system, attr).client
        readSession(client, projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
          .map { session =>
            SDKClientSource.read(client, session).map {
              source =>
                source.map(resp => {
                  if (resp.isArrowRecordBatch) {
                    resp.arrowRecordBatch.get.serializedRecordBatch
                  } else {
                    resp.avroRows.get.serializedBinaryRows
                  }
                })
                  .map(_.toByteArray)
                  .map(ByteString.apply)
                  .map(a => um.asScala(a))
                  .mapAsync(parallelism) { a => a }
            }
          }
          .map(a => a.reduceOption((a, b) => a.merge(b)))
          .filter(a => a.isDefined)
          .flatMapConcat(a => a.get)
      }
    }
  }

  /**
   * Create a source that contains a number of sources, one for each stream, or section of the table data.
   * These sources will emit one GenericRecord for each row within that stream.
   *
   * @param projectId     the projectId the table is located in
   * @param datasetId     the datasetId the table is located in
   * @param tableId       the table to query
   * @param readOptions   Optional TableReadOptions to reduce the amount of data to return, either by column projection or filtering.
   *                      Without this, the whole table will be streamed
   * @param maxNumStreams An optional max initial number of streams. If unset or zero, the server will provide a value of streams so as to produce reasonable throughput.
   *                      Must be non-negative. The number of streams may be lower than the requested number, depending on the amount parallelism that is reasonable for the table.
   *                      Error will be returned if the max count is greater than the current system max limit of 1,000.
   */
  @Deprecated
  def readAvroOnly(projectId: String,
                   datasetId: String,
                   tableId: String,
                   readOptions: Option[TableReadOptions] = None,
                   maxNumStreams: Int = 0): Source[Source[GenericRecord, NotUsed], Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      val schemaS = Promise[String]
      val decoderFlow: Flow[AvroRows, List[GenericRecord], Future[NotUsed]] = Flow.lazyFutureFlow(
        () =>
          schemaS.future.map { ss =>
            val avro = AvroDecoder(ss)
            Flow.fromFunction((r: AvroRows) => avro.decodeRows(r.serializedBinaryRows))
          }(ExecutionContexts.parasitic)
      )

      readSession(client, projectId, datasetId, tableId, DataFormat.AVRO, readOptions, maxNumStreams)
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

  private[scaladsl] def readSession(client: BigQueryReadClient,
                                    projectId: String,
                                    datasetId: String,
                                    tableId: String,
                                    dataFormat: DataFormat,
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
              Some(ReadSession(dataFormat = dataFormat, table = table, readOptions = readOptions)),
              maxNumStreams
            )
          )
      }

  private[scaladsl] def reader(system: ClassicActorSystemProvider, attr: Attributes) =
    attr
      .get[BigQueryStorageAttributes.BigQueryStorageReader]
      .map(_.client)
      .getOrElse(GrpcBigQueryStorageReaderExt()(system).reader)
}
