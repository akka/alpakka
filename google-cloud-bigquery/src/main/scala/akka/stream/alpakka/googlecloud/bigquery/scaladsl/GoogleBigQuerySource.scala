/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.{FromByteStringUnmarshaller, Unmarshaller}
import akka.http.scaladsl.util.FastFuture
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.client.QueryJsonProtocol.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.TableDataQueryResponse
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.TableListQueryResponse
import akka.stream.alpakka.googlecloud.bigquery.client._
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryStreamSource
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.util.ConcatWithHeaders
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryConfig, BigQueryJsonProtocol}
import akka.stream.scaladsl.Source
import spray.json.{JsObject, JsValue, JsonFormat}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Scala API to create BigQuery sources.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object GoogleBigQuerySource {

  /**
   * Create a `GoogleBigQuerySource` that reads elements of `T`.
   * Using this method enables the compiler to automatically infer the JSON type `J`.
   */
  def apply[T]: GoogleBigQuerySource[T] = new GoogleBigQuerySource[T]

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[J, T](
      httpRequest: HttpRequest,
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit jsonUnmarshaller: FromByteStringUnmarshaller[J],
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
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[T](
      httpRequest: HttpRequest,
      parserFn: JsObject => Try[T],
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  ): Source[T, NotUsed] = {
    import SprayJsonSupport._
    implicit val unmarshaller = unmarshallerFromParser(parserFn)
    raw[JsValue, T](httpRequest, onFinishCallback, projectConfig)
  }

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[J, T](query: String, onFinishCallback: PagingInfo => NotUsed, projectConfig: BigQueryConfig)(
      implicit jsonUnmarshaller: FromByteStringUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      rowsUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.ResponseRows[T]]
  ): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        {
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
          BigQueryStreamSource[J, BigQueryJsonProtocol.ResponseRows[T]](request,
                                                                        onFinishCallback,
                                                                        projectConfig,
                                                                        Http())
            .mapConcat(_.rows.map(_.toList).getOrElse(Nil))
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
      ): Source[T, NotUsed] = {
    import SprayJsonSupport._
    implicit val format = new JsonFormat[T] {
      override def write(obj: T): JsValue = throw new UnsupportedOperationException
      override def read(json: JsValue): T = parserFn(json.asJsObject).get
    }
    runQuery[JsValue, T](query, onFinishCallback, projectConfig)
  }

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
          import SprayJsonSupport._
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = BigQueryCommunicationHelper.createQueryRequest(query, projectConfig.projectId, dryRun = false)
          BigQueryStreamSource[JsValue, QueryResponse](request, onFinishCallback, projectConfig, Http())
            .map(BigQueryCommunicationHelper.retrieveQueryResultCsvStyle)
            .via(ConcatWithHeaders())
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * List tables on BigQueryConfig.dataset.
   */
  def listTables(projectConfig: BigQueryConfig): Source[Seq[TableListQueryJsonProtocol.QueryTableModel], NotUsed] = {
    import SprayJsonSupport._
    runMetaQuery[JsValue, TableListQueryResponse](
      GoogleEndpoints.tableListUrl(projectConfig.projectId, projectConfig.dataset),
      projectConfig
    ).map(_.tables)
  }

  /**
   * List fields on tableName.
   */
  def listFields(tableName: String,
                 projectConfig: BigQueryConfig): Source[Seq[TableDataQueryJsonProtocol.Field], NotUsed] = {
    import SprayJsonSupport._
    runMetaQuery[JsValue, TableDataQueryResponse](
      GoogleEndpoints.fieldListUrl(projectConfig.projectId, projectConfig.dataset, tableName),
      projectConfig
    ).map(_.schema.fields)
  }

  private def runMetaQuery[J, T](url: String, projectConfig: BigQueryConfig)(
      implicit jsonUnmarshaller: FromByteStringUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, T]
  ): Source[T, NotUsed] = {
    Source
      .fromMaterializer(
        { (mat, _) =>
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          val request = HttpRequest(HttpMethods.GET, url)
          BigQueryStreamSource[J, T](request, BigQueryCallbacks.ignore, projectConfig, Http())
        }
      )
      .mapMaterializedValue(_ => NotUsed)
  }

  private def unmarshallerFromParser[T](parserFn: JsObject => Try[T]): Unmarshaller[JsValue, T] =
    new Unmarshaller[JsValue, T] {
      override def apply(value: JsValue)(implicit ec: ExecutionContext, materializer: Materializer): Future[T] = {
        Future(FastFuture(parserFn(value.asJsObject))).flatten
      }
    }

}

final class GoogleBigQuerySource[T] private () {

  /**
   * Read elements of `T` by executing HttpRequest upon BigQuery API.
   */
  def raw[J](
      httpRequest: HttpRequest,
      onFinishCallback: PagingInfo => NotUsed,
      projectConfig: BigQueryConfig
  )(implicit jsonUnmarshaller: FromByteStringUnmarshaller[J],
    responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
    unmarshaller: Unmarshaller[J, T]): Source[T, NotUsed] =
    GoogleBigQuerySource.raw[J, T](httpRequest, onFinishCallback, projectConfig)

  /**
   * Read elements of `T` by executing `query`.
   */
  def runQuery[J](query: String, onFinishCallback: PagingInfo => NotUsed, projectConfig: BigQueryConfig)(
      implicit jsonUnmarshaller: FromByteStringUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      rowsUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.ResponseRows[T]]
  ): Source[T, NotUsed] = GoogleBigQuerySource.runQuery[J, T](query, onFinishCallback, projectConfig)

}
