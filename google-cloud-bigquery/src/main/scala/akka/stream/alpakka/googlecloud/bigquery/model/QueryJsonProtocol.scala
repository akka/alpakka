/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.DatasetReference
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryApiJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonFormat
import spray.json.RootJsonFormat

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

object QueryJsonProtocol {

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
   * Java API
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
   * Java API
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
