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
import akka.stream.alpakka.google.cloud.bigquery.client._
import akka.stream.alpakka.google.cloud.bigquery.impl.{GoogleSession, GoogleTokenApi}
import akka.stream.javadsl.Source
import spray.json.JsObject

object GoogleBigQuerySource {
  import collection.JavaConverters._
  import scala.compat.java8.FutureConverters._

  def createProjectConfig(clientEmail: String,
                          privateKey: String,
                          projectId: String,
                          dataset: String,
                          actorSystem: ActorSystem): BigQueryProjectConfig = {
    val session = new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()(actorSystem)))
    new BigQueryProjectConfig(projectId, dataset, session)
  }

  def raw[T](httpRequest: HttpRequest,
             parserFn: java.util.function.Function[JsObject, T],
             googleSession: GoogleSession,
             actorSystem: ActorSystem,
             materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .raw(httpRequest, parserFn.apply, googleSession)(materializer, actorSystem)
      .asJava

  def runQuery[T](query: String,
                  parserFn: java.util.function.Function[JsObject, T],
                  projectConfig: BigQueryProjectConfig,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Source[T, NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQuery(query, parserFn.apply, projectConfig)(materializer, actorSystem)
      .asJava

  def runQueryCsvStyle(query: String,
                       projectConfig: BigQueryProjectConfig,
                       actorSystem: ActorSystem,
                       materializer: Materializer): Source[util.List[String], NotUsed] =
    bigquery.scaladsl.GoogleBigQuerySource
      .runQueryCsvStyle(query, projectConfig)(materializer, actorSystem)
      .map(_.asJava)
      .asJava

  def listTables(projectConfig: BigQueryProjectConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableListQueryJsonProtocol.QueryTableModel]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listTables(projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava

  def listFields(tableName: String,
                 projectConfig: BigQueryProjectConfig,
                 actorSystem: ActorSystem,
                 materializer: Materializer): CompletionStage[util.List[TableDataQueryJsonProtocol.Field]] =
    bigquery.scaladsl.GoogleBigQuerySource
      .listFields(tableName, projectConfig)(materializer, actorSystem, actorSystem.dispatcher)
      .map(_.asJava)(actorSystem.dispatcher)
      .toJava
}
