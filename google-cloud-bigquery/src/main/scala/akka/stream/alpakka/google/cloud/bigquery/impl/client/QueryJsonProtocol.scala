/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.client

import spray.json.{DefaultJsonProtocol, JsBoolean, JsNull, JsObject, JsString, JsValue, JsonFormat}

object QueryJsonProtocol extends DefaultJsonProtocol {

  case class QueryRequest(query: String,
                          useLegacySql: Boolean = false,
                          maxResults: Option[Int] = None,
                          dryRun: Option[Boolean] = None)

  case class QueryResponse(schema: Schema, rows: Option[Seq[Row]])
  case class Schema(fields: Seq[FieldSchema])
  case class FieldSchema(name: String)
  case class Row(f: Seq[RowValue])
  case class RowValue(v: String)

  implicit val queryRequestFormat: JsonFormat[QueryRequest] = jsonFormat4(QueryRequest)

  implicit val rowValueFormat: JsonFormat[RowValue] = new JsonFormat[RowValue] {
    override def write(obj: RowValue): JsValue = JsObject("v" -> JsString(obj.v))

    override def read(json: JsValue): RowValue = json.asJsObject.getFields("v") match {
      case Seq(JsString(value)) =>
        if (value == "true") RowValue("1")
        else if (value == "false") RowValue("0")
        else RowValue(value)
      case Seq(JsNull) => RowValue(null)
      case Seq(JsBoolean(b)) => RowValue(if (b) "1" else "0")
      case Seq(value) => RowValue(value.toString)
      case _ => RowValue(json.toString)
    }
  }
  implicit val rowFormat: JsonFormat[Row] = jsonFormat1(Row)
  implicit val fieldFormat: JsonFormat[FieldSchema] = jsonFormat1(FieldSchema)
  implicit val schemaFormat: JsonFormat[Schema] = jsonFormat1(Schema)
  implicit val queryResponseFormat: JsonFormat[QueryResponse] = jsonFormat2(QueryResponse)

}
