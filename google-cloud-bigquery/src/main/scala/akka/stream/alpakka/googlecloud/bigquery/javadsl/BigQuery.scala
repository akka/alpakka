/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.actor.ClassicActorSystemProvider
import akka.http.javadsl.marshalling.Marshaller
import akka.http.javadsl.model.{HttpEntity, HttpRequest, HttpResponse, RequestEntity}
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.{model => sm}
import akka.japi.function.Function
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{Job, JobCancelResponse, JobReference}
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.{
  TableDataInsertAllRequest,
  TableDataInsertAllResponse,
  TableDataListResponse
}
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  Table,
  TableListResponse,
  TableReference,
  TableSchema
}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQuery => ScalaBigQuery}
import akka.stream.alpakka.googlecloud.bigquery.{BigQuerySettings, InsertAllRetryPolicy}
import akka.stream.javadsl.{Flow, Sink, Source}
import akka.stream.{scaladsl => ss}
import akka.util.ByteString
import akka.{Done, NotUsed}

import java.time.Duration
import java.util.concurrent.CompletionStage
import java.{lang, util}
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.language.implicitConversions

/**
 * Java API to interface with BigQuery.
 */
object BigQuery {

  def getSettings(system: ClassicActorSystemProvider): BigQuerySettings =
    ScalaBigQuery.settings(system)

  def getSettings(system: ClassicActorSystemProvider, prefix: String): BigQuerySettings =
    ScalaBigQuery.settings(prefix)(system)

  def singleRequest(request: HttpRequest,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[HttpResponse] =
    ScalaBigQuery.singleRequest(request)(system, settings).mapTo[HttpResponse].toJava

  def paginatedRequest[Out <: Paginated](request: HttpRequest,
                                         initialPageToken: util.Optional[String],
                                         unmarshaller: Unmarshaller[HttpResponse, Out]): Source[Out, NotUsed] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.paginatedRequest[Out](request, initialPageToken.asScala).asJava
  }

  private implicit def requestAsScala(request: HttpRequest): sm.HttpRequest =
    request.asInstanceOf[sm.HttpRequest]

  def listDatasets(maxResults: util.OptionalInt,
                   all: util.Optional[lang.Boolean],
                   filter: util.Map[String, String]): Source[Dataset, NotUsed] =
    ScalaBigQuery.datasets(maxResults.asScala, all.asScala.map(_.booleanValue), filter.asScala.toMap).asJava

  def getDataset(datasetId: String,
                 system: ClassicActorSystemProvider,
                 settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.dataset(datasetId)(system, settings).toJava

  def createDataset(datasetId: String,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.createDataset(datasetId)(system, settings).toJava

  def createDataset(dataset: Dataset,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.createDataset(dataset)(system, settings).toJava

  def deleteDataset(datasetId: String,
                    deleteContents: Boolean,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Done] =
    ScalaBigQuery.deleteDataset(datasetId, deleteContents)(system, settings).toJava

  def listTables(datasetId: String, maxResults: util.OptionalInt): Source[Table, CompletionStage[TableListResponse]] =
    ScalaBigQuery.tables(datasetId, maxResults.asScala).mapMaterializedValue(_.toJava).asJava

  def getTable(datasetId: String,
               tableId: String,
               system: ClassicActorSystemProvider,
               settings: BigQuerySettings): CompletionStage[Table] =
    ScalaBigQuery.table(datasetId, tableId)(system, settings).toJava

  def createTable(datasetId: String,
                  tableId: String,
                  schema: TableSchema,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Table] =
    createTable(Table(TableReference(None, datasetId, tableId), None, Some(schema), None, None), system, settings)

  def createTable(table: Table,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Table] =
    ScalaBigQuery.createTable(table)(system, settings).toJava

  def deleteTable(datasetId: String,
                  tableId: String,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Done] =
    ScalaBigQuery.deleteTable(datasetId, tableId)(system, settings).toJava

  def listTableData[Out](
      datasetId: String,
      tableId: String,
      startIndex: util.OptionalLong,
      maxResults: util.OptionalInt,
      selectedFields: util.List[String],
      unmarshaller: Unmarshaller[HttpEntity, TableDataListResponse[Out]]
  ): Source[Out, CompletionStage[TableDataListResponse[Out]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery
      .tableData(datasetId, tableId, startIndex.asScala, maxResults.asScala, selectedFields.asScala.toList)
      .mapMaterializedValue(_.toJava)
      .asJava
  }

  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryPolicy: InsertAllRetryPolicy,
      templateSuffix: util.Optional[String],
      marshaller: Marshaller[TableDataInsertAllRequest[In], RequestEntity]
  ): Sink[util.List[In], NotUsed] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    ss.Flow[util.List[In]]
      .map(_.asScala.toList)
      .to(ScalaBigQuery.insertAll[In](datasetId, tableId, retryPolicy, templateSuffix.asScala))
      .asJava[util.List[In]]
  }

  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryFailedRequests: Boolean,
      marshaller: Marshaller[TableDataInsertAllRequest[In], RequestEntity]
  ): Flow[TableDataInsertAllRequest[In], TableDataInsertAllResponse, NotUsed] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    ScalaBigQuery.insertAll[In](datasetId, tableId, retryFailedRequests).asJava
  }

  def getJob(jobId: String,
             location: util.Optional[String],
             system: ClassicActorSystemProvider,
             settings: BigQuerySettings): CompletionStage[Job] =
    ScalaBigQuery.job(jobId, location.asScala)(system, settings).toJava

  def cancelJob(jobId: String,
                location: util.Optional[String],
                system: ClassicActorSystemProvider,
                settings: BigQuerySettings): CompletionStage[JobCancelResponse] =
    ScalaBigQuery.cancelJob(jobId, location.asScala)(system, settings).toJava

  def query[Out](
      query: String,
      dryRun: Boolean,
      useLegacySql: Boolean,
      onCompleteCallback: Function[util.Optional[JobReference], CompletionStage[Done]],
      unmarshaller: Unmarshaller[HttpEntity, QueryResponse[Out]]
  ): Source[Out, CompletionStage[QueryResponse[Out]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.query(query, dryRun, useLegacySql, onCompleteCallback).mapMaterializedValue(_.toJava).asJava
  }

  def query[Out](
      query: QueryRequest,
      onCompleteCallback: Function[util.Optional[JobReference], CompletionStage[Done]],
      unmarshaller: Unmarshaller[HttpEntity, QueryResponse[Out]]
  ): Source[Out, CompletionStage[QueryResponse[Out]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.query(query, onCompleteCallback).mapMaterializedValue(_.toJava).asJava
  }

  def getQueryResults[Out](
      jobId: String,
      startIndex: util.OptionalLong,
      maxResults: util.OptionalInt,
      timeout: util.Optional[Duration],
      location: util.Optional[String],
      unmarshaller: Unmarshaller[HttpEntity, QueryResponse[Out]]
  ): Source[Out, CompletionStage[QueryResponse[Out]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery
      .queryResults(jobId,
                    startIndex.asScala,
                    maxResults.asScala,
                    timeout.asScala.map(d => FiniteDuration(d.toMillis, MILLISECONDS)),
                    location.asScala)
      .mapMaterializedValue(_.toJava)
      .asJava
  }

  private implicit def wrapCallback(
      callback: Function[util.Optional[JobReference], CompletionStage[Done]]
  ): Option[JobReference] => Future[Done] =
    jobReference => callback(jobReference.asJava).toScala

  def insertAllAsync[In](datasetId: String,
                         tableId: String,
                         marshaller: Marshaller[In, RequestEntity]): Flow[In, Job, NotUsed] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    ScalaBigQuery.insertAllAsync[In](datasetId, tableId).asJava[In]
  }

  def createLoadJob[Job](job: Job,
                         marshaller: Marshaller[Job, RequestEntity],
                         unmarshaller: Unmarshaller[HttpEntity, Job]): Sink[ByteString, CompletionStage[Job]] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.createLoadJob(job).mapMaterializedValue(_.toJava).asJava[ByteString]
  }

}
