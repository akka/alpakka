/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl
import java.util

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.http.scaladsl.model.HttpRequest
import akka.stream.alpakka.googlecloud.bigquery
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client._
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.javadsl.Source
import spray.json.JsObject

import scala.util.Try

/**
 * Java API to create BigQuery sources.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object GoogleBigQuerySource {
  import collection.JavaConverters._

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[T](httpRequest: HttpRequest,
             parserFn: java.util.function.Function[JsObject, Try[T]],
             onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
             projectConfig: BigQueryConfig): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .raw(
        httpRequest,
        parserFn.apply(_),
        onFinishCallback.apply,
        projectConfig
      )
      .asJava

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[T](query: String,
                  parserFn: java.util.function.Function[JsObject, Try[T]],
                  onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                  projectConfig: BigQueryConfig): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQuery(query, parserFn.apply(_), onFinishCallback.apply, projectConfig)
      .asJava

  /**
   * Read results in a csv format by executing `query`.
   */
  def runQueryCsvStyle(query: String,
                       onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                       projectConfig: BigQueryConfig): Source[util.List[String], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQueryCsvStyle(query, onFinishCallback.apply, projectConfig)
      .map(_.asJava)
      .asJava

  /**
   * List tables on BigQueryConfig.dataset.
   */
  def listTables(
      projectConfig: BigQueryConfig
  ): Source[util.List[TableListQueryJsonProtocol.QueryTableModel], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listTables(projectConfig)
      .map(_.asJava)
      .asJava

  /**
   * List fields on tableName.
   */
  def listFields(tableName: String,
                 projectConfig: BigQueryConfig): Source[util.List[TableDataQueryJsonProtocol.Field], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listFields(tableName, projectConfig)
      .map(_.asJava)
      .asJava
}
