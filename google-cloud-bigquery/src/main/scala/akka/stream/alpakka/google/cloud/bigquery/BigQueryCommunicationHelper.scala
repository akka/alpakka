/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.alpakka.google.cloud.bigquery.impl.client.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableDataQueryJsonProtocol.TableDataQueryResponse
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableListQueryJsonProtocol.TableListQueryResponse
import akka.stream.alpakka.google.cloud.bigquery.impl.client.{GoogleEndpoints, TableListQueryJsonProtocol}
import spray.json.JsObject

import scala.util.Try

object BigQueryCommunicationHelper {

  def createQueryRequest(query: String, projectId: String, dryRun: Boolean) =
    HttpRequest(HttpMethods.POST, GoogleEndpoints.queryUrl(projectId), entity = createQueryBody(query, dryRun))

  def createQueryBody(query: String, dryRun: Boolean) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query, dryRun = Some(dryRun)).toJson.compactPrint)

  def parseQueryResult(result: JsObject): Option[(Seq[String], Seq[Seq[String]])] =
    Try { result.convertTo[QueryResponse] }.map { queryResponse =>
      val fields = queryResponse.schema.fields.map(_.name)
      val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

      (fields, rows)
    }.toOption

  def parseTableListResult(result: JsObject): Option[Seq[TableListQueryJsonProtocol.QueryTableModel]] =
    Try(result.convertTo[TableListQueryResponse].tables).toOption

  def parseFieldListResults(result: JsObject) =
    Try(result.convertTo[TableDataQueryResponse].schema.fields).toOption
}
