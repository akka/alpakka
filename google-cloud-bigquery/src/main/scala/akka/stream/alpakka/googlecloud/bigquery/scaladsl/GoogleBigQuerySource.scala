/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client._
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.util.ConcatWithHeaders
import akka.stream.alpakka.googlecloud.bigquery.impl.{BigQueryStreamSource, GoogleSession, GoogleTokenApi}
import akka.stream.scaladsl.{Sink, Source}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

object GoogleBigQuerySource {

  def raw[T](
      httpRequest: HttpRequest,
      parserFn: JsObject => Option[T],
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit mat: Materializer, actorSystem: ActorSystem): Source[T, NotUsed] =
    BigQueryStreamSource[T](httpRequest, parserFn, onFinishCallback, projectConfig, Http())

  def runQuery[T](query: String,
                  parserFn: JsObject => Option[T],
                  onFinishCallback: PagingInfo => NotUsed,
                  projectConfig: BigQueryConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem
  ): Source[T, NotUsed] = {
    val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
    BigQueryStreamSource(request, parserFn, onFinishCallback, projectConfig, Http())
  }

  def runQueryCsvStyle(
      query: String,
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit mat: Materializer, actorSystem: ActorSystem): Source[Seq[String], NotUsed] = {
    val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
    BigQueryStreamSource(request, BigQueryCommunicationHelper.parseQueryResult, onFinishCallback, projectConfig, Http())
      .via(ConcatWithHeaders())
  }

  def listTables(projectConfig: BigQueryConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[TableListQueryJsonProtocol.QueryTableModel]] =
    runMetaQuery(GoogleEndpoints.tableListUrl(projectConfig.projectId, projectConfig.dataset),
                 BigQueryCommunicationHelper.parseTableListResult,
                 projectConfig)

  def listFields(tableName: String, projectConfig: BigQueryConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[TableDataQueryJsonProtocol.Field]] =
    runMetaQuery(
      GoogleEndpoints.fieldListUrl(projectConfig.projectId, projectConfig.dataset, tableName),
      BigQueryCommunicationHelper.parseFieldListResults,
      projectConfig
    )

  private def runMetaQuery[T](url: String, parser: JsObject => Option[Seq[T]], projectConfig: BigQueryConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[T]] = {
    val request = HttpRequest(HttpMethods.GET, url)
    val bigQuerySource = BigQueryStreamSource(request, parser, BigQueryCallbacks.ignore, projectConfig, Http())

    bigQuerySource
      .runWith(Sink.seq)
      .map(_.flatten)
  }
}
