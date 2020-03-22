/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.alpakka.googlecloud.bigquery.client.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.TableDataQueryResponse
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.TableListQueryResponse
import spray.json.JsObject

import scala.util.Try

object BigQueryCommunicationHelper {

  def createQueryRequest(query: String, projectId: String, dryRun: Boolean) =
    HttpRequest(HttpMethods.POST, GoogleEndpoints.queryUrl(projectId), entity = createQueryBody(query, dryRun))

  def createQueryBody(query: String, dryRun: Boolean) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query, dryRun = Some(dryRun)).toJson.compactPrint)

  def parseQueryResult(result: JsObject): Try[(Seq[String], Seq[Seq[String]])] =
    Try {
      val queryResponse = result.convertTo[QueryResponse]
      val fields = queryResponse.schema.fields.map(_.name)
      val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

      (fields, rows)
    }

  def parseTableListResult(result: JsObject): Try[Seq[TableListQueryJsonProtocol.QueryTableModel]] =
    Try(result.convertTo[TableListQueryResponse].tables)

  def parseFieldListResults(result: JsObject): Try[Seq[TableDataQueryJsonProtocol.Field]] =
    Try(result.convertTo[TableDataQueryResponse].schema.fields)
}
