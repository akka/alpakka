/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.client

import spray.json.{DefaultJsonProtocol, JsonFormat}

object TableDataQueryJsonProtocol extends DefaultJsonProtocol {

  case class TableDataQueryResponse(schema: TableSchema)
  case class TableSchema(fields: Seq[Field])
  case class Field(name: String, `type`: String)

  implicit val fieldFormat: JsonFormat[Field] = jsonFormat2(Field)
  implicit val tableSchemaFormat: JsonFormat[TableSchema] = jsonFormat1(TableSchema)
  implicit val tableDataQueryResponseFormat: JsonFormat[TableDataQueryResponse] = jsonFormat1(TableDataQueryResponse)
}
