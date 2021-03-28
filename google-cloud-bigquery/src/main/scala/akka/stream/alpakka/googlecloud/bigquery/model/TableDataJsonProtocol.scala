/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.{BigQueryRootJsonReader, BigQueryRootJsonWriter}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation._
import com.github.ghik.silencer.silent
import spray.json.{JsonFormat, RootJsonFormat, RootJsonReader, RootJsonWriter}

import java.{lang, util}
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

/**
 * TableDataListResponse model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list#response-body BigQuery reference]]
 *
 * @param totalRows total rows of the entire table
 * @param pageToken a token indicates from where we should start the next read
 * @param rows repeated rows as result
 * @tparam T the data model of each row
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final case class TableDataListResponse[+T](totalRows: Long, pageToken: Option[String], rows: Option[Seq[T]]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty(value = "totalRows", required = true) totalRows: String,
                   @JsonProperty("pageToken") pageToken: String,
                   @JsonProperty("rows") rows: util.List[T]) =
    this(totalRows.toLong, Option(pageToken), Option(rows).map(_.asScala.toList))

  def getTotalRows = totalRows
  def getPageToken = pageToken.asJava
  def getRows: util.Optional[util.List[T] @uncheckedVariance] = rows.map(_.asJava).asJava

  def withTotalRows(totalRows: Long) =
    copy(totalRows = totalRows)

  def withPageToken(pageToken: Option[String]) =
    copy(pageToken = pageToken)
  def withPageToken(pageToken: util.Optional[String]) =
    copy(pageToken = pageToken.asScala)

  def withRows[S >: T](rows: Option[Seq[S]]) =
    copy(rows = rows)
  def withRows(rows: util.Optional[util.List[T] @uncheckedVariance]) =
    copy(rows = rows.asScala.map(_.asScala.toList))
}

object TableDataListResponse {

  /**
   * Java API: TableDataListResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list#response-body BigQuery reference]]
   *
   * @param totalRows total rows of the entire table
   * @param pageToken a token indicates from where we should start the next read
   * @param rows repeated rows as result
   * @tparam T the data model of each row
   * @return a [[TableDataListResponse]]
   */
  def create[T](totalRows: Long, pageToken: util.Optional[String], rows: util.Optional[util.List[T]]) =
    TableDataListResponse(totalRows, pageToken.asScala, rows.asScala.map(_.asScala.toList))

  implicit def reader[T <: AnyRef](
      implicit reader: BigQueryRootJsonReader[T]
  ): RootJsonReader[TableDataListResponse[T]] = {
    implicit val format = lift(reader)
    jsonFormat3(TableDataListResponse[T])
  }
  implicit val paginated: Paginated[TableDataListResponse[Any]] = _.pageToken
}

/**
 * TableDataInsertAllRequest model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#request-body BigQuery reference]]
 *
 * @param skipInvalidRows insert all valid rows of a request, even if invalid rows exist
 * @param ignoreUnknownValues accept rows that contain values that do not match the schema
 * @param templateSuffix if specified, treats the destination table as a base template, and inserts the rows into an instance table named "{destination}{templateSuffix}"
 * @param rows the rows to insert
 * @tparam T the data model of each row
 */
@JsonInclude(Include.NON_NULL)
final case class TableDataInsertAllRequest[+T](skipInvalidRows: Option[Boolean],
                                               ignoreUnknownValues: Option[Boolean],
                                               templateSuffix: Option[String],
                                               rows: Seq[Row[T]]) {

  @JsonIgnore def getSkipInvalidRows = skipInvalidRows.map(lang.Boolean.valueOf).asJava
  @JsonIgnore def getIgnoreUnknownValues = ignoreUnknownValues.map(lang.Boolean.valueOf).asJava
  @JsonIgnore def getTemplateSuffix = templateSuffix.asJava
  def getRows: util.List[Row[T] @uncheckedVariance] = rows.asJava

  @silent("never used")
  @JsonGetter("skipInvalidRows")
  private def skipInvalidRowsOrNull = skipInvalidRows.map(lang.Boolean.valueOf).orNull
  @silent("never used")
  @JsonGetter("ignoreUnknownValues")
  private def ignoreUnknownValuesOrNull = ignoreUnknownValues.map(lang.Boolean.valueOf).orNull
  @silent("never used")
  @JsonGetter("templateSuffix")
  private def templateSuffixOrNull = templateSuffix.orNull

  def withSkipInvalidRows(skipInvalidRows: Option[Boolean]) =
    copy(skipInvalidRows = skipInvalidRows)
  def withSkipInvalidRows(skipInvalidRows: util.Optional[lang.Boolean]) =
    copy(skipInvalidRows = skipInvalidRows.asScala.map(_.booleanValue))

  def withIgnoreUnknownValues(ignoreUnknownValues: Option[Boolean]) =
    copy(ignoreUnknownValues = ignoreUnknownValues)
  def withIgnoreUnknownValues(ignoreUnknownValues: util.Optional[lang.Boolean]) =
    copy(ignoreUnknownValues = ignoreUnknownValues.asScala.map(_.booleanValue))

  def withTemplateSuffix(templateSuffix: Option[String]) =
    copy(templateSuffix = templateSuffix)
  def withTemplateSuffix(templateSuffix: util.Optional[String]) =
    copy(templateSuffix = templateSuffix.asScala)

  def withRows[S >: T](rows: Seq[Row[S]]) =
    copy(rows = rows)
  def withRows(rows: util.List[Row[T] @uncheckedVariance]) =
    copy(rows = rows.asScala.toList)
}

object TableDataInsertAllRequest {

  /**
   * Java API: TableDataInsertAllRequest model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#request-body BigQuery reference]]
   *
   * @param skipInvalidRows insert all valid rows of a request, even if invalid rows exist
   * @param ignoreUnknownValues accept rows that contain values that do not match the schema
   * @param templateSuffix if specified, treats the destination table as a base template, and inserts the rows into an instance table named "{destination}{templateSuffix}"
   * @param rows the rows to insert
   * @tparam T the data model of each row
   * @return a [[TableDataInsertAllRequest]]
   */
  def create[T](skipInvalidRows: util.Optional[lang.Boolean],
                ignoreUnknownValues: util.Optional[lang.Boolean],
                templateSuffix: util.Optional[String],
                rows: util.List[Row[T]]) =
    TableDataInsertAllRequest(
      skipInvalidRows.asScala.map(_.booleanValue),
      ignoreUnknownValues.asScala.map(_.booleanValue),
      templateSuffix.asScala,
      rows.asScala.toList
    )

  implicit def writer[T](
      implicit writer: BigQueryRootJsonWriter[T]
  ): RootJsonWriter[TableDataInsertAllRequest[T]] = {
    implicit val format = lift(writer)
    implicit val rowFormat = jsonFormat2(Row[T])
    jsonFormat4(TableDataInsertAllRequest[T])
  }
}

/**
 * Row model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#request-body BigQuery reference]]
 *
 * @param insertId a unique ID for deduplication
 * @param json the record this row contains
 * @tparam T the data model of the record
 */
final case class Row[+T](insertId: Option[String], json: T) {

  def getInsertId = insertId.asJava
  def getJson = json

  def withInsertId(insertId: Option[String]) =
    copy(insertId = insertId)
  def withInsertId(insertId: util.Optional[String]) =
    copy(insertId = insertId.asScala)

  def withJson[U >: T](json: U): Row[U] =
    copy(json = json)
}

object Row {

  /**
   * Java API: Row model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#request-body BigQuery reference]]
   *
   * @param insertId a unique ID for deduplication
   * @param json the record this row contains
   * @tparam T the data model of the record
   * @return a [[Row]]
   */
  def create[T](insertId: util.Optional[String], json: T) =
    Row(insertId.asScala, json)
}

/**
 * TableDataInsertAllResponse model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response-body BigQuery reference]]
 */
final case class TableDataInsertAllResponse(insertErrors: Option[Seq[InsertError]]) {
  def getInsertErrors = insertErrors.map(_.asJava).asJava

  def withInsertErrors(insertErrors: Option[Seq[InsertError]]) =
    copy(insertErrors = insertErrors)

  def withInsertErrors(insertErrors: util.Optional[util.List[InsertError]]) =
    copy(insertErrors = insertErrors.asScala.map(_.asScala.toList))
}

object TableDataInsertAllResponse {

  /**
   * Java API: TableDataInsertAllResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response-body BigQuery reference]]
   */
  def create(insertErrors: util.Optional[util.List[InsertError]]) =
    TableDataInsertAllResponse(insertErrors.asScala.map(_.asScala.toList))

  implicit val format: RootJsonFormat[TableDataInsertAllResponse] =
    jsonFormat1(apply)
}

/**
 * InsertError model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response-body BigQuery reference]]
 */
final case class InsertError(index: Int, errors: Option[Seq[ErrorProto]]) {
  def getIndex = index
  def getErrors = errors.map(_.asJava).asJava

  def withIndex(index: Int) =
    copy(index = index)

  def withErrors(errors: Option[Seq[ErrorProto]]) =
    copy(errors = errors)
  def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
    copy(errors = errors.asScala.map(_.asScala.toList))
}

object InsertError {

  /**
   * Java API: InsertError model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response-body BigQuery reference]]
   */
  def create(index: Int, errors: util.Optional[util.List[ErrorProto]]) =
    InsertError(index, errors.asScala.map(_.asScala.toList))

  implicit val format: JsonFormat[InsertError] = jsonFormat2(apply)
}
