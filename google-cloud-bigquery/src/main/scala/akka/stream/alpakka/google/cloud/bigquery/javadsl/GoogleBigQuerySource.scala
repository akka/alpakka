/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.javadsl
import java.util
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.Materializer
import akka.stream.alpakka.google.cloud.bigquery
import akka.stream.alpakka.google.cloud.bigquery.BigQueryFlowModels.BigQueryProjectConfig
import akka.stream.alpakka.google.cloud.bigquery.impl.client._
import akka.stream.alpakka.google.cloud.bigquery.impl.{GoogleSession, GoogleTokenApi}
import akka.stream.javadsl.Source
import spray.json.JsObject

object GoogleBigQuerySource {
  import collection.JavaConverters._
  import scala.compat.java8.FutureConverters._
  import scala.compat.java8.OptionConverters._

  def createSession(clientEmail: String, privateKey: String)(implicit actorSystem: ActorSystem): GoogleSession =
    new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()))

  def createProjectConfig(clientEmail: String, privateKey: String, projectId: String, dataset: String)(
      implicit actorSystem: ActorSystem
  ): BigQueryProjectConfig = {
    val session = createSession(clientEmail, privateKey)
    new BigQueryProjectConfig(projectId, dataset, session)
  }

  def raw[T](httpRequest: HttpRequest,
             parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
             googleSession: GoogleSession,
             materializer: Materializer,
             actorSystem: ActorSystem): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .raw(httpRequest, parserFn.apply(_).asScala, googleSession)(materializer, actorSystem)
      .asJava

  def runQuery[T](query: String,
                  parserFn: java.util.function.Function[JsObject, java.util.Optional[T]],
                  projectConfig: BigQueryProjectConfig,
                  materializer: Materializer,
                  actorSystem: ActorSystem): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQuery(query, parserFn.apply(_).asScala, projectConfig)(materializer, actorSystem)
      .asJava

  def runQueryCsvStyle(query: String,
                       projectConfig: BigQueryProjectConfig,
                       materializer: Materializer,
                       actorSystem: ActorSystem): Source[util.List[String], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQueryCsvStyle(query, projectConfig)(materializer, actorSystem)
      .map(_.asJava)
      .asJava

  def listTables(projectConfig: BigQueryProjectConfig,
                 materializer: Materializer,
                 actorSystem: ActorSystem): CompletionStage[util.List[TableListQueryJsonProtocol.QueryTableModel]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listTables(projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava

  def listFields(tableName: String,
                 projectConfig: BigQueryProjectConfig,
                 materializer: Materializer,
                 actorSystem: ActorSystem): CompletionStage[util.List[TableDataQueryJsonProtocol.Field]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listFields(tableName, projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava
}
