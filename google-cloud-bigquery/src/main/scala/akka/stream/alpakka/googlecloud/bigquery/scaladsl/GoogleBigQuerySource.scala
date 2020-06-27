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
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryStreamSource
import akka.stream.scaladsl.{Sink, Source}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Scala API to create BigQuery sources.
 */
object GoogleBigQuerySource {

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[T](
      httpRequest: HttpRequest,
      parserFn: JsObject => Try[T],
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  ): Source[T, NotUsed] =
    Source
      .setup { (mat, attr) =>
        {
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          BigQueryStreamSource[T](httpRequest, parserFn, onFinishCallback, projectConfig, Http())
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[T](query: String,
                  parserFn: JsObject => Try[T],
                  onFinishCallback: PagingInfo => NotUsed,
                  projectConfig: BigQueryConfig)(
      ): Source[T, NotUsed] =
    Source
      .setup { (mat, attr) =>
        {
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
          BigQueryStreamSource(request, parserFn, onFinishCallback, projectConfig, Http())
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Read results in a csv format by executing `query`.
   */
  def runQueryCsvStyle(
      query: String,
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit mat: Materializer, actorSystem: ActorSystem): Source[Seq[String], NotUsed] = {
    val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
    BigQueryStreamSource(request, BigQueryCommunicationHelper.parseQueryResult, onFinishCallback, projectConfig, Http())
      .via(ConcatWithHeaders())
  }

  /**
   * List tables on BigQueryConfig.dataset.
   */
  def listTables(projectConfig: BigQueryConfig)(
      implicit mat: Materializer,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext
  ): Future[Seq[TableListQueryJsonProtocol.QueryTableModel]] =
    runMetaQuery(GoogleEndpoints.tableListUrl(projectConfig.projectId, projectConfig.dataset),
                 BigQueryCommunicationHelper.parseTableListResult,
                 projectConfig)

  /**
   * List fields on tableName.
   */
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

  private def runMetaQuery[T](url: String, parser: JsObject => Try[Seq[T]], projectConfig: BigQueryConfig)(
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
