/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.client._
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryStreamSource
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.util.ConcatWithHeaders
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryConfig, BigQueryJsonProtocol}
import akka.stream.scaladsl.Source
import spray.json.JsValue

/**
 * Scala API to create BigQuery sources.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object GoogleBigQuerySource {

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[J, T](
      httpRequest: HttpRequest,
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit jsonUnmarshaller: FromEntityUnmarshaller[J],
    responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
    unmarshaller: Unmarshaller[J, T]): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        {
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          BigQueryStreamSource[J, T](httpRequest, onFinishCallback, projectConfig, Http())
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[J, T](query: String, onFinishCallback: PagingInfo => NotUsed, projectConfig: BigQueryConfig)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, T]
  ): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        {
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
          BigQueryStreamSource[J, T](request, onFinishCallback, projectConfig, Http())
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
  ): Source[Seq[String], NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        {
          import BigQueryCommunicationHelper._
          import SprayJsonSupport._

          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
          BigQueryStreamSource[JsValue, (Seq[String], Seq[Seq[String]])](request,
                                                                         onFinishCallback,
                                                                         projectConfig,
                                                                         Http())
            .via(ConcatWithHeaders())
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * List tables on BigQueryConfig.dataset.
   */
  def listTables(projectConfig: BigQueryConfig): Source[Seq[TableListQueryJsonProtocol.QueryTableModel], NotUsed] = {
    import BigQueryCommunicationHelper._
    import SprayJsonSupport._
    runMetaQuery[JsValue, TableListQueryJsonProtocol.QueryTableModel](
      GoogleEndpoints.tableListUrl(projectConfig.projectId, projectConfig.dataset),
      projectConfig
    )
  }

  /**
   * List fields on tableName.
   */
  def listFields(tableName: String,
                 projectConfig: BigQueryConfig): Source[Seq[TableDataQueryJsonProtocol.Field], NotUsed] = {
    import BigQueryCommunicationHelper._
    import SprayJsonSupport._
    runMetaQuery[JsValue, TableDataQueryJsonProtocol.Field](
      GoogleEndpoints.fieldListUrl(projectConfig.projectId, projectConfig.dataset, tableName),
      projectConfig
    )
  }

  private def runMetaQuery[J, T](url: String, projectConfig: BigQueryConfig)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, Seq[T]]
  ): Source[Seq[T], NotUsed] = {
    Source
      .fromMaterializer(
        { (mat, _) =>
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = HttpRequest(HttpMethods.GET, url)
          BigQueryStreamSource[J, Seq[T]](request, BigQueryCallbacks.ignore, projectConfig, Http())
        }
      )
      .mapMaterializedValue(_ => NotUsed)
  }

}
