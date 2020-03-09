/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.japi.function.Creator
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AvroDecoder
import akka.stream.javadsl.{Flow, Source}
import akka.stream.scaladsl.{Flow => SFlow}
import akka.stream.{ActorMaterializer, Attributes}
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1._
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

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
   * @param tableId   the table to query
   */
  def read(projectId: String,
           datasetId: String,
           tableId: String): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, None, 0)

  /**
   * Create a source that contains a number of sources, one for each stream, or section of the table data.
   * These sources will emit one GenericRecord for each row within that stream.
   *
   * @param projectId   the projectId the table is located in
   * @param datasetId   the datasetId the table is located in
   * @param tableId     the table to query
   * @param readOptions TableReadOptions to reduce the amount of data to return, either by column projection or filtering
   */
  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: TableReadOptions): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, Some(readOptions), 0)

  /**
   * Create a source that contains a number of sources, one for each stream, or section of the table data.
   * These sources will emit one GenericRecord for each row within that stream.
   *
   * @param projectId   the projectId the table is located in
   * @param datasetId   the datasetId the table is located in
   * @param tableId     the table to query
   * @param readOptions TableReadOptions to reduce the amount of data to return, either by column projection or filtering
   * @param maxNumStreams An optional max initial number of streams. If unset or zero, the server will provide a value of streams so as to produce reasonable throughput.
   *                      Must be non-negative. The number of streams may be lower than the requested number, depending on the amount parallelism that is reasonable for the table.
   *                      Error will be returned if the max count is greater than the current system max limit of 1,000.
   */
  def read(projectId: String,
           datasetId: String,
           tableId: String,
           readOptions: TableReadOptions,
           maxNumStreams: Int): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, Some(readOptions), maxNumStreams)

  private[this] def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] =
    Source.setup[Source[GenericRecord, NotUsed], NotUsed] { (mat: ActorMaterializer, attr: Attributes) =>
      val client = reader(mat, attr).client
      val schemaS: CompletableFuture[String] = new CompletableFuture[String]()

      val decoderFlow = Flow.lazyInitAsync[AvroRows, List[GenericRecord], NotUsed] {
        new Creator[CompletionStage[Flow[AvroRows, List[GenericRecord], NotUsed]]] {
          override def create(): CompletionStage[Flow[AvroRows, List[GenericRecord], NotUsed]] =
            schemaS.thenApply[Flow[AvroRows, List[GenericRecord], NotUsed]](
              { ss: String =>
                val avro = AvroDecoder(ss)
                SFlow
                  .fromFunction[AvroRows, List[GenericRecord]](r => avro.decodeRows(r.getSerializedBinaryRows))
                  .asJava[AvroRows]
              }.asJava
            )
        }
      }

      Source
        .fromCompletionStage(
          client
            .createReadSession(
              CreateReadSessionRequest
                .newBuilder()
                .setParent(s"projects/$projectId")
                .setReadSession {
                  val builder = ReadSession
                    .newBuilder()
                    .setDataFormat(DataFormat.AVRO)
                    .setTable(s"projects/$projectId/datasets/$datasetId/tables/$tableId")
                  readOptions.fold(builder)(builder.setReadOptions).build()
                }
                .setMaxStreamCount(maxNumStreams)
                .build()
            )
        )
        .mapConcat(japiFunction { session: ReadSession =>
          schemaS.complete(session.getAvroSchema.getSchema)
          session.getStreamsList
        })
        .map(japiFunction { stream =>
          client
            .readRows(ReadRowsRequest.newBuilder().setReadStream(stream.getName).build())
            .map[AvroRows](japiFunction((resp: ReadRowsResponse) => resp.getAvroRows))
            .via(decoderFlow)
            .mapConcat(japiFunction(_.asJava))
        })
    }

  private def reader(mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[BigQueryStorageAttributes.BigQueryStorageReader]
      .map(_.client)
      .getOrElse(GrpcBigQueryStorageReaderExt()(mat.system).reader)

  /**
   * Helper for creating akka.japi.function.Function instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def japiFunction[A, B](f: A => B): akka.japi.function.Function[A, B] =
    new akka.japi.function.Function[A, B]() {
      override def apply(a: A): B = f(a)
    }
}
