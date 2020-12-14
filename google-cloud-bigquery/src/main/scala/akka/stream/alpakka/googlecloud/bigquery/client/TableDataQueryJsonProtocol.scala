/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client

import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object TableDataQueryJsonProtocol extends DefaultJsonProtocol {

  case class TableDataQueryResponse(schema: TableSchema)
  case class TableSchema(fields: Seq[Field])
  case class Field(name: String, `type`: String)

  implicit val fieldFormat: JsonFormat[Field] = jsonFormat2(Field)
  implicit val tableSchemaFormat: JsonFormat[TableSchema] = jsonFormat1(TableSchema)
  implicit val tableDataQueryResponseFormat: RootJsonFormat[TableDataQueryResponse] = jsonFormat1(
    TableDataQueryResponse
  )
}
