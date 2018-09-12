/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.client
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.alpakka.google.cloud.bigquery.client.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.google.cloud.bigquery.client.TableDataQueryJsonProtocol.TableDataQueryResponse
import akka.stream.alpakka.google.cloud.bigquery.client.TableListQueryJsonProtocol.TableListQueryResponse
import spray.json.JsObject

object BigQueryCommunicationHelper {

  def createQueryRequest(query: String, projectId: String, dryRun: Boolean) =
    HttpRequest(HttpMethods.POST, GoogleEndpoints.queryUrl(projectId), entity = createQueryBody(query, dryRun))

  def createQueryBody(query: String, dryRun: Boolean) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query, dryRun = Some(dryRun)).toJson.compactPrint)

  def parseQueryResult(result: JsObject): (Seq[String], Seq[Seq[String]]) = {
    val queryResponse = result.convertTo[QueryResponse]
    val fields = queryResponse.schema.fields.map(_.name)
    val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

    (fields, rows)
  }

  def parseTableListResult(result: JsObject): Seq[TableListQueryJsonProtocol.QueryTableModel] =
    result.convertTo[TableListQueryResponse].tables

  def parseFieldListResults(result: JsObject): Seq[TableDataQueryJsonProtocol.Field] =
    result.convertTo[TableDataQueryResponse].schema.fields
}
