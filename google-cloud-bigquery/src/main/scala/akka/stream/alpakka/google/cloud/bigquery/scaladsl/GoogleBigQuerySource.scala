/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.Materializer
import akka.stream.alpakka.google.cloud.bigquery.BigQueryFlowModels.BigQueryProjectConfig
import akka.stream.alpakka.google.cloud.bigquery.client._
import akka.stream.alpakka.google.cloud.bigquery.impl.util.ConcatWithHeaders
import akka.stream.alpakka.google.cloud.bigquery.impl.{BigQueryStreamSource, GoogleSession, GoogleTokenApi}
import akka.stream.scaladsl.{Sink, Source}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

object GoogleBigQuerySource {

  def createProjectConfig(clientEmail: String, privateKey: String, projectId: String, dataset: String)(
      implicit actorSystem: ActorSystem
  ): BigQueryProjectConfig = {
    val session = new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()))
    new BigQueryProjectConfig(projectId, dataset, session)
  }

  def raw[T](httpRequest: HttpRequest,
             parserFn: JsObject => T,
             googleSession: GoogleSession)(implicit mat: Materializer, actorSystem: ActorSystem): Source[T, NotUsed] =
    BigQueryStreamSource[T](httpRequest, parserFn, googleSession, Http())

  def runQuery[T](query: String, parserFn: JsObject => T, projectConfig: BigQueryProjectConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem
  ): Source[T, NotUsed] = {
    val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
    BigQueryStreamSource(request, parserFn, projectConfig.session, Http())
  }

  def runQueryCsvStyle(
      query: String,
      projectConfig: BigQueryProjectConfig
  )(implicit mat: Materializer, actorSystem: ActorSystem): Source[Seq[String], NotUsed] = {
    val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
    BigQueryStreamSource(request, BigQueryCommunicationHelper.parseQueryResult, projectConfig.session, Http())
      .via(ConcatWithHeaders())
  }

  def listTables(projectConfig: BigQueryProjectConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[TableListQueryJsonProtocol.QueryTableModel]] =
    runMetaQuery(GoogleEndpoints.tableListUrl(projectConfig.projectId, projectConfig.dataset),
                 BigQueryCommunicationHelper.parseTableListResult,
                 projectConfig.session)

  def listFields(tableName: String, projectConfig: BigQueryProjectConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[TableDataQueryJsonProtocol.Field]] =
    runMetaQuery(
      GoogleEndpoints.fieldListUrl(projectConfig.projectId, projectConfig.dataset, tableName),
      BigQueryCommunicationHelper.parseFieldListResults,
      projectConfig.session
    )

  private def runMetaQuery[T](url: String, parser: JsObject => Seq[T], session: GoogleSession)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[T]] = {
    val request = HttpRequest(HttpMethods.GET, url)
    val bigQuerySource = BigQueryStreamSource(request, parser, session, Http())

    bigQuerySource
      .runWith(Sink.seq)
      .map(_.flatten)
  }
}
