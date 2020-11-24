/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.alpakka.googlecloud.bigquery.client.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.TableDataQueryResponse
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.TableListQueryResponse
import spray.json.JsValue

object BigQueryCommunicationHelper {

  def createQueryRequest(query: String, projectId: String, dryRun: Boolean) =
    HttpRequest(HttpMethods.POST, GoogleEndpoints.queryUrl(projectId), entity = createQueryBody(query, dryRun))

  def createQueryBody(query: String, dryRun: Boolean) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query, dryRun = Some(dryRun)).toJson.compactPrint)

  implicit def queryResultUnmarshaller(
      implicit unmarshaller: Unmarshaller[JsValue, QueryResponse]
  ): Unmarshaller[JsValue, (Seq[String], Seq[Seq[String]])] = {
    unmarshaller.map { queryResponse =>
      val fields = queryResponse.schema.fields.map(_.name)
      val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

      (fields, rows)
    }
  }

  implicit def tableListResultUnmarshaller(
      implicit unmarshaller: Unmarshaller[JsValue, TableListQueryResponse]
  ): Unmarshaller[JsValue, Seq[TableListQueryJsonProtocol.QueryTableModel]] = {
    unmarshaller.map(_.tables)
  }

  implicit def fieldListResultsUnmarshaller(
      implicit unmarshaller: Unmarshaller[JsValue, TableDataQueryResponse]
  ): Unmarshaller[JsValue, Seq[TableDataQueryJsonProtocol.Field]] = {
    unmarshaller.map(_.schema.fields)
  }
}
