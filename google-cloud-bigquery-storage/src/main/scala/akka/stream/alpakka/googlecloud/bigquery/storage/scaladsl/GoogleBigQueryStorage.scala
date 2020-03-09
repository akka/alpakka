/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AvroDecoder
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorMaterializer, Attributes}
import com.google.cloud.bigquery.storage.v1.avro.AvroRows
import com.google.cloud.bigquery.storage.v1.storage.{CreateReadSessionRequest, ReadRowsRequest}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{Future, Promise}

/**
 * Google BigQuery Storage Api Akka Stream operator factory.
 */
object GoogleBigQueryStorage {

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
    Source.setup { (mat, attr) =>
      val client = reader(mat, attr).client
      val schemaS = Promise[String]
      val decoderFlow: Flow[AvroRows, List[GenericRecord], Future[Option[NotUsed]]] = Flow.lazyInitAsync(
        () =>
          schemaS.future.map { ss =>
            val avro = AvroDecoder(ss)
            Flow.fromFunction((r: AvroRows) => avro.decodeRows(r.serializedBinaryRows))
          }(ExecutionContexts.sameThreadExecutionContext)
      )

      Source
        .fromFuture(
          client
            .createReadSession(
              CreateReadSessionRequest(
                parent = s"projects/$projectId",
                Some(
                  ReadSession(dataFormat = DataFormat.AVRO,
                              table = s"projects/$projectId/datasets/$datasetId/tables/$tableId",
                              readOptions = readOptions)
                ),
                maxStreamCount = maxNumStreams
              )
            )
        )
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
              .readRows(ReadRowsRequest(stream.name))
              .mapConcat(_.rows.avroRows.toList)
              .via(decoderFlow)
              .mapConcat(identity)
        )
    }

  private def reader(mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[BigQueryStorageAttributes.BigQueryStorageReader]
      .map(_.client)
      .getOrElse(GrpcBigQueryStorageReaderExt()(mat.system).reader)
}
