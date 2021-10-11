/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.javadsl.Source
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import akka.stream.alpakka.googlecloud.bigquery.storage.{scaladsl => scstorage}
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}

import java.util.concurrent.CompletionStage
import collection.JavaConverters._
import scala.compat.java8.FutureConverters.FutureOps

/**
 * Google BigQuery Storage Api Akka Stream operator factory using Arrow Format.
 */
object BigQueryArrowStorage {

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String): Source[java.util.List[BigQueryRecord], CompletionStage[NotUsed]] =
    readRecordsMerged(projectId, datasetId, tableId)

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String,
                        maxNumStreams: Int): Source[java.util.List[BigQueryRecord], CompletionStage[NotUsed]] =
    readRecordsMerged(projectId, datasetId, tableId, None, maxNumStreams)

  def readRecordsMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions
  ): Source[java.util.List[BigQueryRecord], CompletionStage[NotUsed]] =
    readRecordsMerged(projectId, datasetId, tableId, Some(readOptions), 0)

  def readRecordsMerged(projectId: String,
                        datasetId: String,
                        tableId: String,
                        readOptions: TableReadOptions,
                        maxNumStreams: Int): Source[java.util.List[BigQueryRecord], CompletionStage[NotUsed]] =
    readRecordsMerged(projectId, datasetId, tableId, Some(readOptions), maxNumStreams)

  private def readRecordsMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[java.util.List[BigQueryRecord], CompletionStage[NotUsed]] =
    scstorage.BigQueryArrowStorage
      .readRecordsMerged(projectId, datasetId, tableId, readOptions, maxNumStreams)
      .map(stream => {
        stream.asJava
      })
      .asJava
      .mapMaterializedValue(_.toJava)

  def readRecords(projectId: String,
                  datasetId: String,
                  tableId: String): Source[java.util.List[Source[BigQueryRecord, NotUsed]], CompletionStage[NotUsed]] =
    readRecords(projectId, datasetId, tableId)

  def readRecords(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions
  ): Source[java.util.List[Source[BigQueryRecord, NotUsed]], CompletionStage[NotUsed]] =
    readRecords(projectId, datasetId, tableId, Some(readOptions), 0)

  def readRecords(
      projectId: String,
      datasetId: String,
      tableId: String,
      maxNumStreams: Int
  ): Source[java.util.List[Source[BigQueryRecord, NotUsed]], CompletionStage[NotUsed]] =
    readRecords(projectId, datasetId, tableId, None, maxNumStreams)

  def readRecords(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions,
      maxNumStreams: Int
  ): Source[java.util.List[Source[BigQueryRecord, NotUsed]], CompletionStage[NotUsed]] =
    readRecords(projectId, datasetId, tableId, Some(readOptions), maxNumStreams)

  private def readRecords(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[java.util.List[Source[BigQueryRecord, NotUsed]], CompletionStage[NotUsed]] =
    scstorage.BigQueryArrowStorage
      .readRecords(projectId, datasetId, tableId, readOptions, maxNumStreams)
      .map(stream => {
        stream.map(_.asJava).asJava
      })
      .asJava
      .mapMaterializedValue(_.toJava)

  def readMerged(projectId: String,
                 datasetId: String,
                 tableId: String): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), CompletionStage[NotUsed]] =
    readMerged(projectId, datasetId, tableId)

  def readMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      maxNumStreams: Int
  ): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), CompletionStage[NotUsed]] =
    readMerged(projectId, datasetId, tableId, None, maxNumStreams)

  def readMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions
  ): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), CompletionStage[NotUsed]] =
    readMerged(projectId, datasetId, tableId, Some(readOptions), 0)

  def readMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions,
      maxNumStreams: Int
  ): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), CompletionStage[NotUsed]] =
    readMerged(projectId, datasetId, tableId, Some(readOptions), maxNumStreams)

  private def readMerged(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[(ArrowSchema, Source[ArrowRecordBatch, NotUsed]), CompletionStage[NotUsed]] =
    scstorage.BigQueryArrowStorage
      .readMerged(projectId, datasetId, tableId, readOptions, maxNumStreams)
      .map(stream => {
        (stream._1, stream._2.asJava)
      })
      .asJava
      .mapMaterializedValue(_.toJava)

  def read(
      projectId: String,
      datasetId: String,
      tableId: String
  ): Source[(ArrowSchema, java.util.List[Source[ArrowRecordBatch, NotUsed]]), CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, None, 0)

  def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      maxNumStreams: Int
  ): Source[(ArrowSchema, java.util.List[Source[ArrowRecordBatch, NotUsed]]), CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, None, maxNumStreams)

  def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions
  ): Source[(ArrowSchema, java.util.List[Source[ArrowRecordBatch, NotUsed]]), CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, Some(readOptions), 0)

  def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: TableReadOptions,
      maxNumStreams: Int
  ): Source[(ArrowSchema, java.util.List[Source[ArrowRecordBatch, NotUsed]]), CompletionStage[NotUsed]] =
    read(projectId, datasetId, tableId, Some(readOptions), maxNumStreams)

  private def read(
      projectId: String,
      datasetId: String,
      tableId: String,
      readOptions: Option[TableReadOptions],
      maxNumStreams: Int
  ): Source[(ArrowSchema, java.util.List[Source[ArrowRecordBatch, NotUsed]]), CompletionStage[NotUsed]] =
    scstorage.BigQueryArrowStorage
      .read(projectId, datasetId, tableId, readOptions, maxNumStreams)
      .map(stream => {
        (stream._1, stream._2.map(_.asJava).asJava)
      })
      .asJava
      .mapMaterializedValue(_.toJava)

}
