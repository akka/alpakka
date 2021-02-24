/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.DatasetReference
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonFormat
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnoreProperties, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.RootJsonFormat

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

object QueryJsonProtocol {

  /**
   * QueryRequest model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#queryrequest BigQuery reference]]
   *
   * @param query a query string, following the BigQuery query syntax, of the query to execute
   * @param maxResults the maximum number of rows of data to return per page of results
   * @param defaultDataset specifies the default datasetId and projectId to assume for any unqualified table names in the query
   * @param timeoutMs specifies the maximum amount of time, in milliseconds, that the client is willing to wait for the query to complete
   * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
   * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
   * @param requestId a unique user provided identifier to ensure idempotent behavior for queries
   */
  final case class QueryRequest(query: String,
                                maxResults: Option[Int],
                                defaultDataset: Option[DatasetReference],
                                timeoutMs: Option[Int],
                                dryRun: Option[Boolean],
                                useLegacySql: Option[Boolean],
                                requestId: Option[String]) {

    def getQuery = query
    def getMaxResults = maxResults.asPrimitive
    def getDefaultDataset = defaultDataset.asJava
    def getTimeoutMs = timeoutMs.asPrimitive
    def getDryRun = dryRun.map(lang.Boolean.valueOf).asJava
    def getUseLegacySql = useLegacySql.map(lang.Boolean.valueOf).asJava
    def getRequestId = requestId.asJava

    def withQuery(query: String) =
      copy(query = query)
    def withMaxResults(maxResults: util.OptionalInt) =
      copy(maxResults = maxResults.asScala)
    def withDefaultDataset(defaultDataset: util.Optional[DatasetReference]) =
      copy(defaultDataset = defaultDataset.asScala)
    def withTimeoutMs(timeoutMs: util.OptionalInt) =
      copy(timeoutMs = timeoutMs.asScala)
    def withDryRun(dryRun: util.Optional[lang.Boolean]) =
      copy(dryRun = dryRun.asScala.map(_.booleanValue))
    def withUseLegacySql(useLegacySql: util.Optional[lang.Boolean]) =
      copy(useLegacySql = useLegacySql.asScala.map(_.booleanValue))
    def withRequestId(requestId: util.Optional[String]) =
      copy(requestId = requestId.asScala)
  }

  /**
   * Java API: QueryRequest model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#queryrequest BigQuery reference]]
   *
   * @param query a query string, following the BigQuery query syntax, of the query to execute
   * @param maxResults the maximum number of rows of data to return per page of results
   * @param defaultDataset specifies the default datasetId and projectId to assume for any unqualified table names in the query
   * @param timeoutMs specifies the maximum amount of time, in milliseconds, that the client is willing to wait for the query to complete
   * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
   * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
   * @param requestId a unique user provided identifier to ensure idempotent behavior for queries
   * @return a [[QueryRequest]]
   */
  def createQueryRequest(query: String,
                         maxResults: util.OptionalInt,
                         defaultDataset: util.Optional[DatasetReference],
                         timeoutMs: util.OptionalInt,
                         dryRun: util.Optional[lang.Boolean],
                         useLegacySql: util.Optional[lang.Boolean],
                         requestId: util.Optional[String]) =
    QueryRequest(
      query,
      maxResults.asScala,
      defaultDataset.asScala,
      timeoutMs.asScala,
      dryRun.asScala.map(_.booleanValue),
      useLegacySql.asScala.map(_.booleanValue),
      requestId.asScala
    )

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
  final case class QueryResponse[T](schema: Option[TableSchema],
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
    def getRows = rows.map(_.asJava).asJava
    def getTotalBytesProcessed = totalBytesProcessed.asPrimitive
    def getJobComplete = jobComplete
    def getErrors = errors.map(_.asJava).asJava
    def getCacheHit = cacheHit.map(lang.Boolean.valueOf).asJava
    def getNumDmlAffectedRows = numDmlAffectedRows.asPrimitive

    def withSchema(schema: util.Optional[TableSchema]) =
      copy(schema = schema.asScala)
    def withJobReference(jobReference: JobReference) =
      copy(jobReference = jobReference)
    def withTotalRows(totalRows: util.OptionalLong) =
      copy(totalRows = totalRows.asScala)
    def withPageToken(pageToken: util.Optional[String]) =
      copy(pageToken = pageToken.asScala)
    def withRows(rows: util.Optional[util.List[T]]) =
      copy(rows = rows.asScala.map(_.asScala.toList))
    def withTotalBytesProcessed(totalBytesProcessed: util.OptionalLong) =
      copy(totalBytesProcessed = totalBytesProcessed.asScala)
    def withJobComplete(jobComplete: Boolean) =
      copy(jobComplete = jobComplete)
    def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
      copy(errors = errors.asScala.map(_.asScala.toList))
    def withCacheHit(cacheHit: util.Optional[lang.Boolean]) =
      copy(cacheHit = cacheHit.asScala.map(_.booleanValue))
    def withNumDmlAffectedRows(numDmlAffectedRows: util.OptionalLong) =
      copy(numDmlAffectedRows = numDmlAffectedRows.asScala)
  }

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
  def createQueryResponse[T](schema: util.Optional[TableSchema],
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

  implicit val requestFormat: RootJsonFormat[QueryRequest] = jsonFormat7(QueryRequest)
  implicit def responseFormat[T: BigQueryRootJsonFormat]: RootJsonFormat[QueryResponse[T]] =
    jsonFormat10(QueryResponse[T])
  implicit def paginated[T]: Paginated[QueryResponse[T]] = _.pageToken
}
