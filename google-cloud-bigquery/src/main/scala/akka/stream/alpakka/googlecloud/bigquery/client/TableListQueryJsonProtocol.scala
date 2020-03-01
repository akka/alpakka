/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client

import spray.json.{DefaultJsonProtocol, JsonFormat}

object TableListQueryJsonProtocol extends DefaultJsonProtocol {

  case class TableListQueryResponse(tables: Seq[QueryTableModel])
  case class QueryTableModel(tableReference: TableReference, `type`: String)
  case class TableReference(tableId: String)

  implicit val tableReferenceFormat: JsonFormat[TableReference] = jsonFormat1(TableReference)
  implicit val queryTableModelFormat: JsonFormat[QueryTableModel] = jsonFormat2(QueryTableModel)
  implicit val tableListQueryResponseFormat: JsonFormat[TableListQueryResponse] = jsonFormat1(TableListQueryResponse)
}
