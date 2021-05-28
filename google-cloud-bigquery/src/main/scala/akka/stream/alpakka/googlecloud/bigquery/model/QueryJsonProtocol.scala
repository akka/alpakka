/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonReader
import akka.util.JavaDurationConverters._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnoreProperties, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.{RootJsonFormat, RootJsonReader}

import java.time.Duration
import java.{lang, util}
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * QueryRequest model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#queryrequest BigQuery reference]]
 *
 * @param query a query string, following the BigQuery query syntax, of the query to execute
 * @param maxResults the maximum number of rows of data to return per page of results
 * @param defaultDataset specifies the default datasetId and projectId to assume for any unqualified table names in the query
 * @param timeout specifies the maximum amount of time that the client is willing to wait for the query to complete
 * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
 * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
 * @param location the geographic location where the job should run
 * @param labels the labels associated with this query
 * @param maximumBytesBilled limits the number of bytes billed for this query
 * @param requestId a unique user provided identifier to ensure idempotent behavior for queries
 */
final case class QueryRequest private (query: String,
                                       maxResults: Option[Int],
                                       defaultDataset: Option[DatasetReference],
                                       timeout: Option[FiniteDuration],
                                       dryRun: Option[Boolean],
                                       useLegacySql: Option[Boolean],
                                       location: Option[String],
                                       labels: Map[String, String],
                                       maximumBytesBilled: Option[Long],
                                       requestId: Option[String]) {

  def getQuery = query
  def getMaxResults = maxResults.asPrimitive
  def getDefaultDataset = defaultDataset.asJava
  def getTimeout = timeout.map(_.asJava).asJava
  def getDryRun = dryRun.map(lang.Boolean.valueOf).asJava
  def getUseLegacySql = useLegacySql.map(lang.Boolean.valueOf).asJava
  def getRequestId = requestId.asJava
  def getLocation = location.asJava
  def getMaximumBytesBilled = maximumBytesBilled.asJava
  def getLabels = labels.asJava

  def withQuery(query: String) =
    copy(query = query)

  def withMaxResults(maxResults: Option[Int]) =
    copy(maxResults = maxResults)
  def withMaxResults(maxResults: util.OptionalInt) =
    copy(maxResults = maxResults.asScala)

  def withDefaultDataset(defaultDataset: Option[DatasetReference]) =
    copy(defaultDataset = defaultDataset)
  def withDefaultDataset(defaultDataset: util.Optional[DatasetReference]) =
    copy(defaultDataset = defaultDataset.asScala)

  def withTimeout(timeout: Option[FiniteDuration]) =
    copy(timeout = timeout)
  def withTimeout(timeout: util.Optional[Duration]) =
    copy(timeout = timeout.asScala.map(_.asScala))

  def withDryRun(dryRun: Option[Boolean]) =
    copy(dryRun = dryRun)
  def withDryRun(dryRun: util.Optional[lang.Boolean]) =
    copy(dryRun = dryRun.asScala.map(_.booleanValue))

  def withUseLegacySql(useLegacySql: Option[Boolean]) =
    copy(useLegacySql = useLegacySql)
  def withUseLegacySql(useLegacySql: util.Optional[lang.Boolean]) =
    copy(useLegacySql = useLegacySql.asScala.map(_.booleanValue))

  def withRequestId(requestId: Option[String]) =
    copy(requestId = requestId)
  def withRequestId(requestId: util.Optional[String]) =
    copy(requestId = requestId.asScala)

  def withLocation(location: Option[String]) =
    copy(location = location)
  def withLocation(location: util.Optional[String]) =
    copy(location = location.asScala)

  def withMaximumBytesBilled(maximumBytesBilled: Option[Long]) =
    copy(maximumBytesBilled = maximumBytesBilled)
  def withMaximumBytesBilled(maximumBytesBilled: util.OptionalLong) =
    copy(maximumBytesBilled = maximumBytesBilled.asScala)

  def withLabels(labels: Map[String, String]) =
    copy(labels = labels)
  def withLabels(labels: util.Map[String, String]) =
    copy(labels = labels.asScala.toMap)
}

object QueryRequest {

  def apply(query: String,
            maxResults: Option[Int],
            defaultDataset: Option[DatasetReference],
            timeout: Option[FiniteDuration],
            dryRun: Option[Boolean],
            useLegacySql: Option[Boolean],
            requestId: Option[String]): QueryRequest =
    QueryRequest(query, maxResults, defaultDataset, timeout, dryRun, useLegacySql, None, Map.empty, None, requestId)

  /**
   * Java API: QueryRequest model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#queryrequest BigQuery reference]]
   *
   * @param query a query string, following the BigQuery query syntax, of the query to execute
   * @param maxResults the maximum number of rows of data to return per page of results
   * @param defaultDataset specifies the default datasetId and projectId to assume for any unqualified table names in the query
   * @param timeout specifies the maximum amount of time, in milliseconds, that the client is willing to wait for the query to complete
   * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
   * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
   * @param requestId a unique user provided identifier to ensure idempotent behavior for queries
   * @return a [[QueryRequest]]
   */
  def create(query: String,
             maxResults: util.OptionalInt,
             defaultDataset: util.Optional[DatasetReference],
             timeout: util.Optional[Duration],
             dryRun: util.Optional[lang.Boolean],
             useLegacySql: util.Optional[lang.Boolean],
             requestId: util.Optional[String]) =
    QueryRequest(
      query,
      maxResults.asScala,
      defaultDataset.asScala,
      timeout.asScala.map(_.asScala),
      dryRun.asScala.map(_.booleanValue),
      useLegacySql.asScala.map(_.booleanValue),
      None,
      Map.empty,
      None,
      requestId.asScala
    )

  implicit val format: RootJsonFormat[QueryRequest] = jsonFormat(
    apply,
    "query",
    "maxResults",
    "defaultDataset",
    "timeoutMs",
    "dryRun",
    "useLegacySql",
    "requestId",
    "location",
    "maximumBytesBilled",
    "labels"
  )
}

/**
 * QueryResponse model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#response-body BigQuery reference]]
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body BigQuery reference]]
 *
 * @param schema the schema of the results
 * @param jobReference reference to the Job that was created to run the query
 * @param totalRows the total number of rows in the complete query result set, which can be more than the number of rows in this single page of results
 * @param pageToken a token used for paging results
 * @param rows an object with as many results as can be contained within the maximum permitted reply size
 * @param totalBytesProcessed the total number of bytes processed for this query
 * @param jobComplete whether the query has completed or not
 * @param errors the first errors or warnings encountered during the running of the job
 * @param cacheHit whether the query result was fetched from the query cache
 * @param numDmlAffectedRows the number of rows affected by a DML statement
 * @tparam T the data model for each row
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final case class QueryResponse[+T] private (schema: Option[TableSchema],
                                            jobReference: JobReference,
                                            totalRows: Option[Long],
                                            pageToken: Option[String],
                                            rows: Option[Seq[T]],
                                            totalBytesProcessed: Option[Long],
                                            jobComplete: Boolean,
                                            errors: Option[Seq[ErrorProto]],
                                            cacheHit: Option[Boolean],
                                            numDmlAffectedRows: Option[Long]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty("schema") schema: TableSchema,
                   @JsonProperty(value = "jobReference", required = true) jobReference: JobReference,
                   @JsonProperty("totalRows") totalRows: String,
                   @JsonProperty("pageToken") pageToken: String,
                   @JsonProperty("rows") rows: util.List[T],
                   @JsonProperty("totalBytesProcessed") totalBytesProcessed: String,
                   @JsonProperty(value = "jobComplete", required = true) jobComplete: Boolean,
                   @JsonProperty("errors") errors: util.List[ErrorProto],
                   @JsonProperty("cacheHit") cacheHit: lang.Boolean,
                   @JsonProperty("numDmlAffectedRows") numDmlAffectedRows: String) =
    this(
      Option(schema),
      jobReference,
      Option(totalRows).map(_.toLong),
      Option(pageToken),
      Option(rows).map(_.asScala.toList),
      Option(totalBytesProcessed).map(_.toLong),
      jobComplete,
      Option(errors).map(_.asScala.toList),
      Option(cacheHit).map(_.booleanValue),
      Option(numDmlAffectedRows).map(_.toLong)
    )

  def getSchema = schema.asJava
  def getJobReference = jobReference
  def getTotalRows = totalRows.asPrimitive
  def getPageToken = pageToken.asJava
  def getRows: util.Optional[util.List[T] @uncheckedVariance] = rows.map(_.asJava).asJava
  def getTotalBytesProcessed = totalBytesProcessed.asPrimitive
  def getJobComplete = jobComplete
  def getErrors = errors.map(_.asJava).asJava
  def getCacheHit = cacheHit.map(lang.Boolean.valueOf).asJava
  def getNumDmlAffectedRows = numDmlAffectedRows.asPrimitive

  def withSchema(schema: Option[TableSchema]) =
    copy(schema = schema)
  def withSchema(schema: util.Optional[TableSchema]) =
    copy(schema = schema.asScala)

  def withJobReference(jobReference: JobReference) =
    copy(jobReference = jobReference)

  def withTotalRows(totalRows: Option[Long]) =
    copy(totalRows = totalRows)
  def withTotalRows(totalRows: util.OptionalLong) =
    copy(totalRows = totalRows.asScala)

  def withPageToken(pageToken: Option[String]) =
    copy(pageToken = pageToken)
  def withPageToken(pageToken: util.Optional[String]) =
    copy(pageToken = pageToken.asScala)

  def withRows[S >: T](rows: Option[Seq[S]]) =
    copy(rows = rows)
  def withRows(rows: util.Optional[util.List[T] @uncheckedVariance]) =
    copy(rows = rows.asScala.map(_.asScala.toList))

  def withTotalBytesProcessed(totalBytesProcessed: Option[Long]) =
    copy(totalBytesProcessed = totalBytesProcessed)
  def withTotalBytesProcessed(totalBytesProcessed: util.OptionalLong) =
    copy(totalBytesProcessed = totalBytesProcessed.asScala)

  def withJobComplete(jobComplete: Boolean) =
    copy(jobComplete = jobComplete)

  def withErrors(errors: Option[Seq[ErrorProto]]) =
    copy(errors = errors)
  def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
    copy(errors = errors.asScala.map(_.asScala.toList))

  def withCacheHit(cacheHit: Option[Boolean]) =
    copy(cacheHit = cacheHit)
  def withCacheHit(cacheHit: util.Optional[lang.Boolean]) =
    copy(cacheHit = cacheHit.asScala.map(_.booleanValue))

  def withNumDmlAffectedRows(numDmlAffectedRows: Option[Long]) =
    copy(numDmlAffectedRows = numDmlAffectedRows)
  def withNumDmlAffectedRows(numDmlAffectedRows: util.OptionalLong) =
    copy(numDmlAffectedRows = numDmlAffectedRows.asScala)
}

object QueryResponse {

  /**
   * Java API: QueryResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#response-body BigQuery reference]]
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body BigQuery reference]]
   *
   * @param schema the schema of the results
   * @param jobReference reference to the Job that was created to run the query
   * @param totalRows the total number of rows in the complete query result set, which can be more than the number of rows in this single page of results
   * @param pageToken a token used for paging results
   * @param rows an object with as many results as can be contained within the maximum permitted reply size
   * @param totalBytesProcessed the total number of bytes processed for this query
   * @param jobComplete whether the query has completed or not
   * @param errors the first errors or warnings encountered during the running of the job
   * @param cacheHit whether the query result was fetched from the query cache
   * @param numDmlAffectedRows the number of rows affected by a DML statement
   * @tparam T the data model for each row
   * @return a [[QueryResponse]]
   */
  def create[T](schema: util.Optional[TableSchema],
                jobReference: JobReference,
                totalRows: util.OptionalLong,
                pageToken: util.Optional[String],
                rows: util.Optional[util.List[T]],
                totalBytesProcessed: util.OptionalLong,
                jobComplete: Boolean,
                errors: util.Optional[util.List[ErrorProto]],
                cacheHit: util.Optional[lang.Boolean],
                numDmlAffectedRows: util.OptionalLong) =
    QueryResponse[T](
      schema.asScala,
      jobReference,
      totalRows.asScala,
      pageToken.asScala,
      rows.asScala.map(_.asScala.toList),
      totalBytesProcessed.asScala,
      jobComplete,
      errors.asScala.map(_.asScala.toList),
      cacheHit.asScala.map(_.booleanValue),
      numDmlAffectedRows.asScala
    )

  implicit def reader[T <: AnyRef](
      implicit reader: BigQueryRootJsonReader[T]
  ): RootJsonReader[QueryResponse[T]] = {
    implicit val format = lift(reader)
    jsonFormat10(QueryResponse[T])
  }
  implicit val paginated: Paginated[QueryResponse[Any]] = _.pageToken
}
