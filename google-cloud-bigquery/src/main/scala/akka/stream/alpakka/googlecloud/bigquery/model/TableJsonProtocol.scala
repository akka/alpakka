/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object TableJsonProtocol extends DefaultJsonProtocol {

  final case class Table(tableReference: TableReference,
                         labels: Option[Map[String, String]],
                         schema: Option[TableSchema],
                         numRows: Option[String],
                         location: Option[String])
  final case class TableReference(projectId: Option[String], datasetId: String, tableId: String)
  final case class TableSchema(fields: Seq[TableFieldSchema])
  final case class TableFieldSchema(name: String,
                                    `type`: String,
                                    mode: Option[String],
                                    fields: Option[Seq[TableFieldSchema]])
  val StringType = "STRING"
  val BytesType = "BYTES"
  val IntegerType = "INTEGER"
  val FloatType = "FLOAT"
  val BooleanType = "BOOLEAN"
  val TimestampType = "TIMESTAMP"
  val DateType = "DATE"
  val TimeType = "TIME"
  val DateTimeType = "DATETIME"
  val GeographyType = "GEOGRAPHY"
  val NumericType = "NUMERIC"
  val RecordType = "RECORD"
  val NullableMode = "NULLABLE"
  val RequiredMode = "REQUIRED"
  val RepeatedMode = "REPEATED"

  implicit val fieldSchemaFormat: JsonFormat[TableFieldSchema] = lazyFormat(
    jsonFormat(TableFieldSchema, "name", "type", "mode", "fields")
  )
  implicit val schemaFormat: JsonFormat[TableSchema] = jsonFormat1(TableSchema)
  implicit val referenceFormat: JsonFormat[TableReference] = jsonFormat3(TableReference)
  implicit val format: RootJsonFormat[Table] = jsonFormat5(Table)

  final case class TableListResponse(tables: Option[Seq[Table]], totalItems: Option[Int])

  implicit val listResponseFormat: RootJsonFormat[TableListResponse] = jsonFormat2(TableListResponse)
}
