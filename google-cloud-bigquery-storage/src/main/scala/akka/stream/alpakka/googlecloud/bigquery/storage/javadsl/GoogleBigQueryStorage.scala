/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import java.util.concurrent.CompletionStage
import akka.NotUsed
import akka.stream.javadsl.Source
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import org.apache.avro.generic.GenericRecord
import akka.stream.alpakka.googlecloud.bigquery.storage.{scaladsl => scstorage}
import akka.stream.alpakka.googlecloud.bigquery.storage.ProtobufConverters._
import scala.compat.java8.FutureConverters.FutureOps

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

  /**
   * Create a source that contains a number of sources, one for each stream, or section of the table data.
   * These sources will emit one GenericRecord for each row within that stream.
   *
   * @param projectId   the projectId the table is located in
   * @param datasetId   the datasetId the table is located in
   * @param tableId     the table to query
   * @param maxNumStreams An optional max initial number of streams. If unset or zero, the server will provide a value of streams so as to produce reasonable throughput.
   *                      Must be non-negative. The number of streams may be lower than the requested number, depending on the amount parallelism that is reasonable for the table.
   *                      Error will be returned if the max count is greater than the current system max limit of 1,000.
   */
  def read(projectId: String,
           datasetId: String,
           tableId: String,
           maxNumStreams: Int): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, None, maxNumStreams)

  private val RequestParamsHeader = "x-goog-request-params"

  private[this] def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[Source[GenericRecord, NotUsed], CompletionStage[NotUsed]] = {
    val source = scstorage.GoogleBigQueryStorage.read(projectId, datasetId, tableId, readOptions.map(_.asScala()), maxNumStreams)
      .map(s => s.asJava)
      .asJava
      .mapMaterializedValue(_.toJava)
    source
  }

}
