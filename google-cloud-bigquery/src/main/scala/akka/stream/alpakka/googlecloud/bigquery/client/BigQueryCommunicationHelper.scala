/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.alpakka.googlecloud.bigquery.client.QueryJsonProtocol.{QueryRequest, QueryResponse}

object BigQueryCommunicationHelper {

  def createQueryRequest(query: String, projectId: String, dryRun: Boolean) =
    HttpRequest(HttpMethods.POST, GoogleEndpoints.queryUrl(projectId), entity = createQueryBody(query, dryRun))

  def createQueryBody(query: String, dryRun: Boolean) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query, dryRun = Some(dryRun)).toJson.compactPrint)

  def retrieveQueryResultCsvStyle(queryResponse: QueryResponse): (Seq[String], Seq[Seq[String]]) = {
    val fields = queryResponse.schema.fields.map(_.name)
    val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))
    (fields, rows)
  }

}
