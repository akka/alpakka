/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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

object GoogleBigQuerySource {
  import collection.JavaConverters._
  import scala.compat.java8.FutureConverters._
  import scala.compat.java8.OptionConverters._

  def raw[T](httpRequest: HttpRequest,
             parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
             onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
             projectConfig: BigQueryConfig,
             actorSystem: ActorSystem,
             materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .raw(httpRequest, parserFn.apply(_).asScala, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .asJava

  def runQuery[T](query: String,
                  parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
                  onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                  projectConfig: BigQueryConfig,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQuery(query, parserFn.apply(_).asScala, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .asJava

  def runQueryCsvStyle(query: String,
                       onFinishCallback: java.util.function.Function[PagingInfo, NotUsed],
                       projectConfig: BigQueryConfig,
                       actorSystem: ActorSystem,
                       materializer: Materializer): Source[util.List[String], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQueryCsvStyle(query, onFinishCallback.apply, projectConfig)(materializer, actorSystem)
      .map(_.asJava)
      .asJava

  def listTables(projectConfig: BigQueryConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableListQueryJsonProtocol.QueryTableModel]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listTables(projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava

  def listFields(tableName: String,
                 projectConfig: BigQueryConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableDataQueryJsonProtocol.Field]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listFields(tableName, projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava
}
