/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl
import java.util
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client._
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.javadsl.Source
import spray.json.JsObject

/**
 * Java API to create BigQuery sources.
 */
object GoogleBigQuerySource {
  import collection.JavaConverters._
  import scala.compat.java8.FutureConverters._
  import scala.compat.java8.OptionConverters._

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[T](httpRequest: HttpRequest,
             parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
             onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
             projectConfig: BigQueryConfig,
             actorSystem: ActorSystem,
             materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .raw(httpRequest, parserFn.apply(_).asScala, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .asJava

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[T](query: String,
                  parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
                  onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                  projectConfig: BigQueryConfig,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQuery(query, parserFn.apply(_).asScala, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .asJava

  /**
   * Read results in a csv format by executing `query`.
   */
  def runQueryCsvStyle(query: String,
                       onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                       projectConfig: BigQueryConfig,
                       actorSystem: ActorSystem,
                       materializer: Materializer): Source[util.List[String], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQueryCsvStyle(query, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .map(_.asJava)
      .asJava

  /**
   * List tables on BigQueryConfig.dataset.
   */
  def listTables(projectConfig: BigQueryConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableListQueryJsonProtocol.QueryTableModel]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listTables(projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava

  /**
   * List fields on tableName.
   */
  def listFields(tableName: String,
                 projectConfig: BigQueryConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableDataQueryJsonProtocol.Field]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listFields(tableName, projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava
}
