/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonReader
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnoreProperties, JsonProperty}
import spray.json.{RootJsonFormat, RootJsonReader}
import java.time.Duration
import java.{lang, util}

import scala.annotation.nowarn
import scala.annotation.unchecked.uncheckedVariance
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.jdk.OptionConverters._
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
                                       labels: Option[Map[String, String]],
                                       maximumBytesBilled: Option[Long],
                                       requestId: Option[String]) {

  def getQuery = query
  def getMaxResults = maxResults.toJavaPrimitive
  def getDefaultDataset = defaultDataset.toJava
  def getTimeout = timeout.map(_.toJava).toJava
  def getDryRun = dryRun.map(lang.Boolean.valueOf).toJava
  def getUseLegacySql = useLegacySql.map(lang.Boolean.valueOf).toJava
  def getRequestId = requestId.toJava
  def getLocation = location.toJava
  def getMaximumBytesBilled = maximumBytesBilled.toJava
  def getLabels = labels.toJava

  def withQuery(query: String) =
    copy(query = query)

  def withMaxResults(maxResults: Option[Int]) =
    copy(maxResults = maxResults)
  def withMaxResults(maxResults: util.OptionalInt) =
    copy(maxResults = maxResults.toScala)

  def withDefaultDataset(defaultDataset: Option[DatasetReference]) =
    copy(defaultDataset = defaultDataset)
  def withDefaultDataset(defaultDataset: util.Optional[DatasetReference]) =
    copy(defaultDataset = defaultDataset.toScala)

  def withTimeout(timeout: Option[FiniteDuration]) =
    copy(timeout = timeout)
  def withTimeout(timeout: util.Optional[Duration]) =
    copy(timeout = timeout.toScala.map(_.toScala))

  def withDryRun(dryRun: Option[Boolean]) =
    copy(dryRun = dryRun)
  def withDryRun(dryRun: util.Optional[lang.Boolean]) =
    copy(dryRun = dryRun.toScala.map(_.booleanValue))

  def withUseLegacySql(useLegacySql: Option[Boolean]) =
    copy(useLegacySql = useLegacySql)
  def withUseLegacySql(useLegacySql: util.Optional[lang.Boolean]) =
    copy(useLegacySql = useLegacySql.toScala.map(_.booleanValue))

  def withRequestId(requestId: Option[String]) =
    copy(requestId = requestId)
  def withRequestId(requestId: util.Optional[String]) =
    copy(requestId = requestId.toScala)

  def withLocation(location: Option[String]) =
    copy(location = location)
  def withLocation(location: util.Optional[String]) =
    copy(location = location.toScala)

  def withMaximumBytesBilled(maximumBytesBilled: Option[Long]) =
    copy(maximumBytesBilled = maximumBytesBilled)
  def withMaximumBytesBilled(maximumBytesBilled: util.OptionalLong) =
    copy(maximumBytesBilled = maximumBytesBilled.toScala)

  def withLabels(labels: Option[Map[String, String]]) =
    copy(labels = labels)
  def withLabels(labels: util.Optional[util.Map[String, String]]) =
    copy(labels = labels.toScala.map(_.asScala.toMap))
}

object QueryRequest {

  def apply(query: String,
            maxResults: Option[Int],
            defaultDataset: Option[DatasetReference],
            timeout: Option[FiniteDuration],
            dryRun: Option[Boolean],
            useLegacySql: Option[Boolean],
            requestId: Option[String]): QueryRequest =
    QueryRequest(query, maxResults, defaultDataset, timeout, dryRun, useLegacySql, None, None, None, requestId)

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
      maxResults.toScala,
      defaultDataset.toScala,
      timeout.toScala.map(_.toScala),
      dryRun.toScala.map(_.booleanValue),
      useLegacySql.toScala.map(_.booleanValue),
      None,
      None,
      None,
      requestId.toScala
    )

  implicit val format: RootJsonFormat[QueryRequest] = jsonFormat(
    apply,
    "query",
    "maxResults",
    "defaultDataset",
    "timeoutMs",
    "dryRun",
    "useLegacySql",
    "location",
    "labels",
    "maximumBytesBilled",
    "requestId"
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

  @nowarn("msg=never used")
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

  def getSchema = schema.toJava
  def getJobReference = jobReference
  def getTotalRows = totalRows.toJavaPrimitive
  def getPageToken = pageToken.toJava
  def getRows: util.Optional[util.List[T] @uncheckedVariance] = rows.map(_.asJava).toJava
  def getTotalBytesProcessed = totalBytesProcessed.toJavaPrimitive
  def getJobComplete = jobComplete
  def getErrors = errors.map(_.asJava).toJava
  def getCacheHit = cacheHit.map(lang.Boolean.valueOf).toJava
  def getNumDmlAffectedRows = numDmlAffectedRows.toJavaPrimitive

  def withSchema(schema: Option[TableSchema]) =
    copy(schema = schema)
  def withSchema(schema: util.Optional[TableSchema]) =
    copy(schema = schema.toScala)

  def withJobReference(jobReference: JobReference) =
    copy(jobReference = jobReference)

  def withTotalRows(totalRows: Option[Long]) =
    copy(totalRows = totalRows)
  def withTotalRows(totalRows: util.OptionalLong) =
    copy(totalRows = totalRows.toScala)

  def withPageToken(pageToken: Option[String]) =
    copy(pageToken = pageToken)
  def withPageToken(pageToken: util.Optional[String]) =
    copy(pageToken = pageToken.toScala)

  def withRows[S >: T](rows: Option[Seq[S]]) =
    copy(rows = rows)
  def withRows(rows: util.Optional[util.List[T] @uncheckedVariance]) =
    copy(rows = rows.toScala.map(_.asScala.toList))

  def withTotalBytesProcessed(totalBytesProcessed: Option[Long]) =
    copy(totalBytesProcessed = totalBytesProcessed)
  def withTotalBytesProcessed(totalBytesProcessed: util.OptionalLong) =
    copy(totalBytesProcessed = totalBytesProcessed.toScala)

  def withJobComplete(jobComplete: Boolean) =
    copy(jobComplete = jobComplete)

  def withErrors(errors: Option[Seq[ErrorProto]]) =
    copy(errors = errors)
  def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
    copy(errors = errors.toScala.map(_.asScala.toList))

  def withCacheHit(cacheHit: Option[Boolean]) =
    copy(cacheHit = cacheHit)
  def withCacheHit(cacheHit: util.Optional[lang.Boolean]) =
    copy(cacheHit = cacheHit.toScala.map(_.booleanValue))

  def withNumDmlAffectedRows(numDmlAffectedRows: Option[Long]) =
    copy(numDmlAffectedRows = numDmlAffectedRows)
  def withNumDmlAffectedRows(numDmlAffectedRows: util.OptionalLong) =
    copy(numDmlAffectedRows = numDmlAffectedRows.toScala)
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
      schema.toScala,
      jobReference,
      totalRows.toScala,
      pageToken.toScala,
      rows.toScala.map(_.asScala.toList),
      totalBytesProcessed.toScala,
      jobComplete,
      errors.toScala.map(_.asScala.toList),
      cacheHit.toScala.map(_.booleanValue),
      numDmlAffectedRows.toScala
    )

  implicit def reader[T <: AnyRef](
      implicit reader: BigQueryRootJsonReader[T]
  ): RootJsonReader[QueryResponse[T]] = {
    implicit val format = lift(reader)
    jsonFormat10(QueryResponse[T])
  }
  implicit val paginated: Paginated[QueryResponse[Any]] = _.pageToken
}
