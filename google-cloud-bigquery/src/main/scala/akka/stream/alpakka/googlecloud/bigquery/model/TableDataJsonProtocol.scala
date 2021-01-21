/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonFormat
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryApiJsonProtocol._
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonCreator, JsonGetter, JsonIgnore, JsonIgnoreProperties, JsonInclude, JsonProperty}
import spray.json.{JsonFormat, RootJsonFormat}

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

object TableDataJsonProtocol {

  @JsonIgnoreProperties(ignoreUnknown = true)
  final case class TableDataListResponse[T](totalRows: Long, pageToken: Option[String], rows: Option[Seq[T]]) {

    @JsonCreator
    private def this(@JsonProperty(value = "totalRows", required = true) totalRows: String,
                     @JsonProperty("pageToken") pageToken: String,
                     @JsonProperty("rows") rows: util.List[T]) =
      this(totalRows.toLong, Option(pageToken), Option(rows).map(_.asScala.toList))

    def getTotalRows = totalRows
    def getPageToken = pageToken.asJava
    def getRows = rows.map(_.asJava).asJava

    def withTotalRows(totalRows: Long) =
      copy(totalRows = totalRows)
    def withPageToken(pageToken: util.Optional[String]) =
      copy(pageToken = pageToken.asScala)
    def withRows(rows: util.Optional[util.List[T]]) =
      copy(rows = rows.asScala.map(_.asScala.toList))
  }

  /**
   * Java API
   */
  def createTableDataListResponse[T](totalRows: Long,
                                     pageToken: util.Optional[String],
                                     rows: util.Optional[util.List[T]]) =
    TableDataListResponse(totalRows, pageToken.asScala, rows.asScala.map(_.asScala.toList))

  @JsonInclude(Include.NON_NULL)
  final case class TableDataInsertAllRequest[T](skipInvalidRows: Option[Boolean],
                                                ignoreUnknownValues: Option[Boolean],
                                                templateSuffix: Option[String],
                                                rows: Seq[Row[T]]) {

    @JsonIgnore def getSkipInvalidRows = skipInvalidRows.map(lang.Boolean.valueOf).asJava
    @JsonIgnore def getIgnoreUnknownValues = ignoreUnknownValues.map(lang.Boolean.valueOf).asJava
    @JsonIgnore def getTemplateSuffix = templateSuffix.asJava
    def getRows = rows.asJava

    @JsonGetter("skipInvalidRows")
    private def skipInvalidRowsOrNull = skipInvalidRows.map(lang.Boolean.valueOf).orNull
    @JsonGetter("ignoreUnknownValues")
    private def ignoreUnknownValuesOrNull = ignoreUnknownValues.map(lang.Boolean.valueOf).orNull
    @JsonGetter("templateSuffix")
    private def templateSuffixOrNull = templateSuffix.orNull

    def withSkipInvalidRows(skipInvalidRows: util.Optional[lang.Boolean]) =
      copy(skipInvalidRows = skipInvalidRows.asScala.map(_.booleanValue))
    def withIgnoreUnknownValues(ignoreUnknownValues: util.Optional[lang.Boolean]) =
      copy(ignoreUnknownValues = ignoreUnknownValues.asScala.map(_.booleanValue))
    def withTemplateSuffix(templateSuffix: util.Optional[String]) =
      copy(templateSuffix = templateSuffix.asScala)
    def withRows(rows: util.List[Row[T]]): TableDataInsertAllRequest[T] =
      copy(rows = rows.asScala.toList)
  }

  /**
   * Java API
   */
  def createTableDataInsertAllRequest[T](skipInvalidRows: util.Optional[lang.Boolean],
                                         ignoreUnknownValues: util.Optional[lang.Boolean],
                                         templateSuffix: util.Optional[String],
                                         rows: util.List[Row[T]]) =
    TableDataInsertAllRequest(
      skipInvalidRows.asScala.map(_.booleanValue),
      ignoreUnknownValues.asScala.map(_.booleanValue),
      templateSuffix.asScala,
      rows.asScala.toList
    )

  final case class Row[+T](insertId: Option[String], json: T) {

    def getInsertId = insertId.asJava
    def getJson = json

    def withInsertId(insertId: util.Optional[String]) =
      copy(insertId = insertId.asScala)
    def withJson[U >: T](json: U): Row[U] =
      copy(json = json)
  }

  /**
   * Java API
   */
  def createRow[T](insertId: util.Optional[String], json: T) =
    Row(insertId.asScala, json)

  final case class TableDataInsertAllResponse(insertErrors: Option[Seq[InsertError]]) {
    def getInsertErrors = insertErrors.map(_.asJava).asJava
    def withInsertErrors(insertErrors: util.Optional[util.List[InsertError]]) =
      copy(insertErrors = insertErrors.asScala.map(_.asScala.toList))
  }

  /**
   * Java API
   */
  def createTableDataInsertAllResponse(insertErrors: util.Optional[util.List[InsertError]]) =
    TableDataInsertAllResponse(insertErrors.asScala.map(_.asScala.toList))

  final case class InsertError(index: Int, errors: Option[Seq[ErrorProto]]) {
    def getIndex = index
    def getErrors = errors.map(_.asJava).asJava
    def withIndex(index: Int) =
      copy(index = index)
    def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
      copy(errors = errors.asScala.map(_.asScala.toList))
  }

  /**
   * Java API
   */
  def createInsertError(index: Int, errors: util.Optional[util.List[ErrorProto]]) =
    InsertError(index, errors.asScala.map(_.asScala.toList))

  implicit def listResponseFormat[T: BigQueryRootJsonFormat]: RootJsonFormat[TableDataListResponse[T]] =
    jsonFormat3(TableDataListResponse[T])
  implicit def paginated[T]: Paginated[TableDataListResponse[T]] = _.pageToken
  implicit def rowFormat[T: BigQueryRootJsonFormat]: JsonFormat[Row[T]] = jsonFormat2(Row[T])
  implicit def insertAllRequestFormat[T: BigQueryRootJsonFormat]: RootJsonFormat[TableDataInsertAllRequest[T]] =
    jsonFormat4(TableDataInsertAllRequest[T])
  implicit val insertErrorFormat: JsonFormat[InsertError] = jsonFormat2(InsertError)
  implicit val insertAllResponseFormat: RootJsonFormat[TableDataInsertAllResponse] =
    jsonFormat1(TableDataInsertAllResponse)
}
