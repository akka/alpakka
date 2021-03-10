/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.http.javadsl.marshalling.Marshaller
import akka.http.javadsl.model.{HttpEntity, HttpRequest, HttpResponse, RequestEntity}
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.{model => sm}
import akka.japi.Pair
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
import com.github.ghik.silencer.silent

import java.time.Duration
import java.util.concurrent.CompletionStage
import java.{lang, util}
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.language.implicitConversions

/**
 * Java API to interface with BigQuery.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object BigQuery {

  /**
   * Returns the default [[BigQuerySettings]].
   *
   * @param system the actor system
   */
  def getSettings(system: ClassicActorSystemProvider): BigQuerySettings =
    ScalaBigQuery.settings(system)

  /**
   * Returns the [[BigQuerySettings]] defined at a path in the configuration.
   *
   * @param system the actor system
   * @param path the configuration path
   */
  def getSettings(system: ClassicActorSystemProvider, path: String): BigQuerySettings =
    ScalaBigQuery.settings(path)(system)

  /**
   * Makes a single authenticated request without retries.
   *
   * @param request the [[akka.http.javadsl.model.HttpRequest]] to make
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.http.javadsl.model.HttpResponse]]
   */
  def singleRequest(request: HttpRequest,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[HttpResponse] =
    ScalaBigQuery.singleRequest(request)(system, settings).mapTo[HttpResponse].toJava

  /**
   * Makes a series of authenticated requests to page through a resource.
   *
   * @param request the [[akka.http.javadsl.model.HttpRequest]] to make; must be a GET request
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[Out]]
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.javadsl.Source]] that emits an [[Out]] for each page of the resource
   */
  def paginatedRequest[Out <: Paginated](request: HttpRequest,
                                         unmarshaller: Unmarshaller[HttpResponse, Out]): Source[Out, NotUsed] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.paginatedRequest[Out](request).asJava
  }

  private implicit def requestAsScala(request: HttpRequest): sm.HttpRequest =
    request.asInstanceOf[sm.HttpRequest]

  /**
   * Lists all datasets in the specified project to which the user has been granted the READER dataset role.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list BigQuery reference]]
   *
   * @param maxResults the maximum number of results to return in a single response page
   * @param all whether to list all datasets, including hidden ones
   * @param filter a key, value [[java.util.Map]] for filtering the results of the request by label
   * @return a [[akka.stream.javadsl.Source]] that emits each [[akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset]]
   */
  def listDatasets(maxResults: util.OptionalInt,
                   all: util.Optional[lang.Boolean],
                   filter: util.Map[String, String]): Source[Dataset, NotUsed] =
    ScalaBigQuery.datasets(maxResults.asScala, all.asScala.map(_.booleanValue), filter.asScala.toMap).asJava

  /**
   * Returns the specified dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get BigQuery reference]]
   *
   * @param datasetId dataset ID of the requested dataset
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset]]
   */
  def getDataset(datasetId: String,
                 system: ClassicActorSystemProvider,
                 settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.dataset(datasetId)(system, settings).toJava

  /**
   * Creates a new empty dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert BigQuery reference]]
   *
   * @param datasetId dataset ID of the new dataset
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset]]
   */
  def createDataset(datasetId: String,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.createDataset(datasetId)(system, settings).toJava

  /**
   * Creates a new empty dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert BigQuery reference]]
   *
   * @param dataset the [[akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset]] to create
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset]]
   */
  def createDataset(dataset: Dataset,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Dataset] =
    ScalaBigQuery.createDataset(dataset)(system, settings).toJava

  /**
   * Deletes the dataset specified by the datasetId value.
   *
   * @param datasetId dataset ID of dataset being deleted
   * @param deleteContents if `true`, delete all the tables in the dataset; if `false` and the dataset contains tables, the request will fail
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing [[akka.Done]]
   */
  def deleteDataset(datasetId: String,
                    deleteContents: Boolean,
                    system: ClassicActorSystemProvider,
                    settings: BigQuerySettings): CompletionStage[Done] =
    ScalaBigQuery.deleteDataset(datasetId, deleteContents)(system, settings).toJava

  /**
   * Lists all tables in the specified dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list BigQuery reference]]
   *
   * @param datasetId dataset ID of the tables to list
   * @param maxResults the maximum number of results to return in a single response page
   * @return a [[akka.stream.javadsl.Source]] that emits each [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]] in the dataset and materializes a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableListResponse]]
   */
  def listTables(datasetId: String, maxResults: util.OptionalInt): Source[Table, CompletionStage[TableListResponse]] =
    ScalaBigQuery.tables(datasetId, maxResults.asScala).mapMaterializedValue(_.toJava).asJava

  /**
   * Gets the specified table resource. This method does not return the data in the table, it only returns the table resource, which describes the structure of this table.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get BigQuery reference]]
   *
   * @param datasetId dataset ID of the requested table
   * @param tableId table ID of the requested table
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def getTable(datasetId: String,
               tableId: String,
               system: ClassicActorSystemProvider,
               settings: BigQuerySettings): CompletionStage[Table] =
    ScalaBigQuery.table(datasetId, tableId)(system, settings).toJava

  /**
   * Creates a new, empty table in the dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert BigQuery reference]]
   *
   * @param datasetId dataset ID of the new table
   * @param tableId table ID of the new table
   * @param schema [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema]] of the new table
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def createTable(datasetId: String,
                  tableId: String,
                  schema: TableSchema,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Table] =
    createTable(Table(TableReference(None, datasetId, tableId), None, Some(schema), None, None), system, settings)

  /**
   * Creates a new, empty table in the dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert BigQuery reference]]
   *
   * @param table the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]] to create
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def createTable(table: Table,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Table] =
    ScalaBigQuery.createTable(table)(system, settings).toJava

  /**
   * Deletes the specified table from the dataset. If the table contains data, all the data will be deleted.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to delete
   * @param tableId table ID of the table to delete
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing [[akka.Done]]
   */
  def deleteTable(datasetId: String,
                  tableId: String,
                  system: ClassicActorSystemProvider,
                  settings: BigQuerySettings): CompletionStage[Done] =
    ScalaBigQuery.deleteTable(datasetId, tableId)(system, settings).toJava

  /**
   * Lists the content of a table in rows.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to list
   * @param tableId table ID of the table to list
   * @param startIndex start row index of the table
   * @param maxResults row limit of the table
   * @param selectedFields subset of fields to return, supports select into sub fields. Example: `selectedFields = List.of("a", "e.d.f")`
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataListResponse]]
   * @tparam Out the data model of each row
   * @return a [[akka.stream.javadsl.Source]] that emits an [[Out]] for each row in the table
   */
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

  /**
   * Streams data into BigQuery one record at a time without needing to run a load job
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll BigQuery reference]]
   *
   * @param datasetId dataset id of the table to insert into
   * @param tableId table id of the table to insert into
   * @param retryPolicy [[InsertAllRetryPolicy]] determining whether to retry and deduplicate
   * @param templateSuffix if specified, treats the destination table as a base template, and inserts the rows into an instance table named "{destination}{templateSuffix}"
   * @param marshaller [[akka.http.javadsl.marshalling.Marshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllRequest]]
   * @tparam In the data model for each record
   * @return a [[akka.stream.javadsl.Sink]] that inserts each batch of [[In]] into the table
   */
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

  /**
   * Streams data into BigQuery one record at a time without needing to run a load job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to insert into
   * @param tableId table ID of the table to insert into
   * @param retryFailedRequests whether to retry failed requests
   * @param marshaller [[akka.http.javadsl.marshalling.Marshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllRequest]]
   * @tparam In the data model for each record
   * @return a [[akka.stream.javadsl.Flow]] that sends each [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllRequest]] and emits a [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllResponse]] for each
   */
  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryFailedRequests: Boolean,
      marshaller: Marshaller[TableDataInsertAllRequest[In], RequestEntity]
  ): Flow[TableDataInsertAllRequest[In], TableDataInsertAllResponse, NotUsed] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    ScalaBigQuery.insertAll[In](datasetId, tableId, retryFailedRequests).asJava
  }

  /**
   * Runs a BigQuery SQL query.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query BigQuery reference]]
   *
   * @param query a query string, following the BigQuery query syntax, of the query to execute
   * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
   * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.javadsl.Source]] that emits an [[Out]] for each row of the results and materializes
   *         a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
  def query[Out](
      query: String,
      dryRun: Boolean,
      useLegacySql: Boolean,
      unmarshaller: Unmarshaller[HttpEntity, QueryResponse[Out]]
  ): Source[Out, CompletionStage[QueryResponse[Out]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.query(query, dryRun, useLegacySql).mapMaterializedValue(_.toJava).asJava
  }

  /**
   * Runs a BigQuery SQL query.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query BigQuery reference]]
   *
   * @param query the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryRequest]]
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.javadsl.Source]] that emits an [[Out]] for each row of the results and materializes
   *         a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference]]
   *         a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
  def query[Out](
      query: QueryRequest,
      unmarshaller: Unmarshaller[HttpEntity, QueryResponse[Out]]
  ): Source[Out, Pair[CompletionStage[JobReference], CompletionStage[QueryResponse[Out]]]] = {
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery
      .query(query)
      .mapMaterializedValue {
        case (jobReference, queryResponse) =>
          Pair(jobReference.toJava, queryResponse.toJava)
      }
      .asJava
  }

  /**
   * The results of a query job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults BigQuery reference]]
   *
   * @param jobId job ID of the query job
   * @param startIndex zero-based index of the starting row
   * @param maxResults maximum number of results to read
   * @param timeout specifies the maximum amount of time that the client is willing to wait for the query to complete
   * @param location the geographic location of the job. Required except for US and EU
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.javadsl.Source]] that emits an [[Out]] for each row of the results and materializes a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
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

  /**
   * Returns information about a specific job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get BigQuery reference]]
   *
   * @param jobId job ID of the requested job
   * @param location the geographic location of the job. Required except for US and EU
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[Job]]
   */
  def getJob(jobId: String,
             location: util.Optional[String],
             system: ClassicActorSystemProvider,
             settings: BigQuerySettings): CompletionStage[Job] =
    ScalaBigQuery.job(jobId, location.asScala)(system, settings).toJava

  /**
   * Requests that a job be cancelled.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel BigQuery reference]]
   *
   * @param jobId job ID of the job to cancel
   * @param location the geographic location of the job. Required except for US and EU
   * @param system the actor system
   * @param settings the [[akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings]]
   * @return a [[java.util.concurrent.CompletionStage]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobCancelResponse]]
   */
  def cancelJob(jobId: String,
                location: util.Optional[String],
                system: ClassicActorSystemProvider,
                settings: BigQuerySettings): CompletionStage[JobCancelResponse] =
    ScalaBigQuery.cancelJob(jobId, location.asScala)(system, settings).toJava

  /**
   * Loads data into BigQuery via a series of asynchronous load jobs, configurable by [[akka.stream.alpakka.googlecloud.bigquery.LoadJobSettings]].
   * @note WARNING: Pending the resolution of [[https://issuetracker.google.com/176002651 BigQuery issue 176002651]] this method may not work as expected.
   *       As a workaround, you can use the config setting `akka.http.parsing.conflicting-content-type-header-processing-mode = first` with Akka HTTP v10.2.4 or later.
   *
   * @param datasetId dataset ID of the table to insert into
   * @param tableId table ID of the table to insert into
   * @param marshaller [[akka.http.javadsl.marshalling.Marshaller]] for [[In]]
   * @tparam In the data model for each record
   * @return a [[akka.stream.javadsl.Flow]] that uploads each [[In]] and emits a [[Job]] for every upload job created
   */
  def insertAllAsync[In](datasetId: String,
                         tableId: String,
                         marshaller: Marshaller[In, RequestEntity]): Flow[In, Job, NotUsed] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    ScalaBigQuery.insertAllAsync[In](datasetId, tableId).asJava[In]
  }

  /**
   * Starts a new asynchronous upload job.
   * @note WARNING: Pending the resolution of [[https://issuetracker.google.com/176002651 BigQuery issue 176002651]] this method may not work as expected.
   *       As a workaround, you can use the config setting `akka.http.parsing.conflicting-content-type-header-processing-mode = first` with Akka HTTP v10.2.4 or later.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert BigQuery reference]]
   * @see [[https://cloud.google.com/bigquery/docs/reference/api-uploads BigQuery reference]]
   *
   * @param job the job to start
   * @param marshaller [[akka.http.javadsl.marshalling.Marshaller]] for [[Job]]
   * @param unmarshaller [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[Job]]
   * @tparam Job the data model for a job
   * @return a [[akka.stream.javadsl.Sink]] that uploads bytes and materializes a [[java.util.concurrent.CompletionStage]] containing the [[Job]] when completed
   */
  def createLoadJob[@silent("shadows") Job](
      job: Job,
      marshaller: Marshaller[Job, RequestEntity],
      unmarshaller: Unmarshaller[HttpEntity, Job]
  ): Sink[ByteString, CompletionStage[Job]] = {
    implicit val m = marshaller.asScalaCastOutput[sm.RequestEntity]
    implicit val um = unmarshaller.asScalaCastInput[sm.HttpEntity]
    ScalaBigQuery.createLoadJob(job).mapMaterializedValue(_.toJava).asJava[ByteString]
  }

}
