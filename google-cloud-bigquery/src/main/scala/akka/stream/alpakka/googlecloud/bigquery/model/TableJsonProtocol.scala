/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.{JsonFormat, RootJsonFormat}

import java.util
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

/**
 * Table resource model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource:-table BigQuery reference]]
 *
 * @param tableReference reference describing the ID of this table
 * @param labels the labels associated with this table
 * @param schema describes the schema of this table
 * @param numRows the number of rows of data in this table
 * @param location the geographic location where the table resides
 */
final case class Table private (tableReference: TableReference,
                                labels: Option[Map[String, String]],
                                schema: Option[TableSchema],
                                numRows: Option[Long],
                                location: Option[String]) {

  def getTableReference = tableReference
  def getLabels = labels.map(_.asJava).asJava
  def getSchema = schema.asJava
  def getNumRows = numRows.asPrimitive
  def getLocation = location.asJava

  def withTableReference(tableReference: TableReference) =
    copy(tableReference = tableReference)

  def withLabels(labels: Option[Map[String, String]]) =
    copy(labels = labels)
  def withLabels(labels: util.Optional[util.Map[String, String]]) =
    copy(labels = labels.asScala.map(_.asScala.toMap))

  def withSchema(schema: Option[TableSchema]) =
    copy(schema = schema)
  def withSchema(schema: util.Optional[TableSchema]) =
    copy(schema = schema.asScala)

  def withNumRows(numRows: Option[Long]) =
    copy(numRows = numRows)
  def withNumRows(numRows: util.OptionalLong) =
    copy(numRows = numRows.asScala)

  def withLocation(location: Option[String]) =
    copy(location = location)
  def withLocation(location: util.Optional[String]) =
    copy(location = location.asScala)
}

object Table {

  /**
   * Java API: Table resource model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource:-table BigQueryReference]]
   *
   * @param tableReference reference describing the ID of this table
   * @param labels the labels associated with this table
   * @param schema describes the schema of this table
   * @param numRows the number of rows of data in this table
   * @param location the geographic location where the table resides
   * @return a [[Table]]
   */
  def create(tableReference: TableReference,
             labels: util.Optional[util.Map[String, String]],
             schema: util.Optional[TableSchema],
             numRows: util.OptionalLong,
             location: util.Optional[String]) =
    Table(
      tableReference,
      labels.asScala.map(_.asScala.toMap),
      schema.asScala,
      numRows.asScala,
      location.asScala
    )

  implicit val format: RootJsonFormat[Table] = jsonFormat5(apply)
}

/**
 * TableReference model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference BigQuery reference]]
 *
 * @param projectId the ID of the project containing this table
 * @param datasetId the ID of the dataset containing this table
 * @param tableId the ID of the table
 */
final case class TableReference private (projectId: Option[String], datasetId: String, tableId: Option[String]) {

  def getProjectId = projectId.asJava
  def getDatasetId = datasetId
  def getTableId = tableId

  def withProjectId(projectId: Option[String]) =
    copy(projectId = projectId)
  def withProjectId(projectId: util.Optional[String]) =
    copy(projectId = projectId.asScala)

  def withDatasetId(datasetId: String) =
    copy(datasetId = datasetId)

  def withTableId(tableId: Option[String]) =
    copy(tableId = tableId)
  def withTableId(tableId: util.Optional[String]) =
    copy(tableId = tableId.asScala)
}

object TableReference {

  /**
   * Java API: TableReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference BigQuery reference]]
   *
   * @param projectId the ID of the project containing this table
   * @param datasetId the ID of the dataset containing this table
   * @param tableId the ID of the table
   * @return a [[TableReference]]
   */
  def create(projectId: util.Optional[String], datasetId: String, tableId: util.Optional[String]) =
    TableReference(projectId.asScala, datasetId, tableId.asScala)

  implicit val referenceFormat: JsonFormat[TableReference] = jsonFormat3(apply)
}

/**
 * Schema of a table
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
 *
 * @param fields describes the fields in a table
 */
final case class TableSchema private (fields: Seq[TableFieldSchema]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty(value = "fields", required = true) fields: util.List[TableFieldSchema]) =
    this(fields.asScala.toList)

  def getFields = fields.asJava

  def withFields(fields: Seq[TableFieldSchema]) =
    copy(fields = fields)
  def withFields(fields: util.List[TableFieldSchema]) =
    copy(fields = fields.asScala.toList)
}

object TableSchema {

  /**
   * Java API: Schema of a table
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
   *
   * @param fields describes the fields in a table
   * @return a [[TableSchema]]
   */
  def create(fields: util.List[TableFieldSchema]) = TableSchema(fields.asScala.toList)

  /**
   * Java API: Schema of a table
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
   *
   * @param fields describes the fields in a table
   * @return a [[TableSchema]]
   */
  @varargs
  def create(fields: TableFieldSchema*) = TableSchema(fields.toList)

  implicit val format: JsonFormat[TableSchema] = jsonFormat1(apply)
}

/**
 * A field in TableSchema
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema BigQuery reference]]
 *
 * @param name the field name
 * @param `type` the field data type
 * @param mode the field mode
 * @param fields describes the nested schema fields if the type property is set to `RECORD`
 */
final case class TableFieldSchema private (name: String,
                                           `type`: TableFieldSchemaType,
                                           mode: Option[TableFieldSchemaMode],
                                           fields: Option[Seq[TableFieldSchema]]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty(value = "name", required = true) name: String,
                   @JsonProperty(value = "type", required = true) `type`: String,
                   @JsonProperty("mode") mode: String,
                   @JsonProperty("fields") fields: util.List[TableFieldSchema]) =
    this(
      name,
      TableFieldSchemaType(`type`),
      Option(mode).map(TableFieldSchemaMode.apply),
      Option(fields).map(_.asScala.toList)
    )

  def getName = name
  def getType = `type`
  def getMode = mode.asJava
  def getFields = fields.map(_.asJava).asJava

  def withName(name: String) =
    copy(name = name)

  def withType(`type`: TableFieldSchemaType) =
    copy(`type` = `type`)

  def withMode(mode: Option[TableFieldSchemaMode]) =
    copy(mode = mode)
  def withMode(mode: util.Optional[TableFieldSchemaMode]) =
    copy(mode = mode.asScala)

  def withFields(fields: Option[Seq[TableFieldSchema]]) =
    copy(fields = fields)
  def withFields(fields: util.Optional[util.List[TableFieldSchema]]) =
    copy(fields = fields.asScala.map(_.asScala.toList))
}

object TableFieldSchema {

  /**
   * A field in TableSchema
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema BigQuery reference]]
   *
   * @param name the field name
   * @param `type` the field data type
   * @param mode the field mode
   * @param fields describes the nested schema fields if the type property is set to `RECORD`
   * @return a [[TableFieldSchema]]
   */
  def create(name: String,
             `type`: TableFieldSchemaType,
             mode: util.Optional[TableFieldSchemaMode],
             fields: util.Optional[util.List[TableFieldSchema]]) =
    TableFieldSchema(name, `type`, mode.asScala, fields.asScala.map(_.asScala.toList))

  /**
   * A field in TableSchema
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema BigQuery reference]]
   *
   * @param name the field name
   * @param `type` the field data type
   * @param mode the field mode
   * @param fields describes the nested schema fields if the type property is set to `RECORD`
   * @return a [[TableFieldSchema]]
   */
  @varargs
  def create(name: String,
             `type`: TableFieldSchemaType,
             mode: util.Optional[TableFieldSchemaMode],
             fields: TableFieldSchema*) =
    TableFieldSchema(name, `type`, mode.asScala, if (fields.nonEmpty) Some(fields.toList) else None)

  implicit val format: JsonFormat[TableFieldSchema] = lazyFormat(
    jsonFormat(apply, "name", "type", "mode", "fields")
  )
}

final case class TableFieldSchemaType private (value: String) extends StringEnum
object TableFieldSchemaType {

  /**
   * Java API
   */
  def create(value: String) = TableFieldSchemaType(value)

  val String = TableFieldSchemaType("STRING")
  def string = String

  val Bytes = TableFieldSchemaType("BYTES")
  def bytes = Bytes

  val Integer = TableFieldSchemaType("INTEGER")
  def integer = Integer

  val Float = TableFieldSchemaType("FLOAT")
  def float64 = Float // float is a reserved keyword in Java

  val Boolean = TableFieldSchemaType("BOOLEAN")
  def bool = Boolean // boolean is a reserved keyword in Java

  val Timestamp = TableFieldSchemaType("TIMESTAMP")
  def timestamp = Timestamp

  val Date = TableFieldSchemaType("DATE")
  def date = Date

  val Time = TableFieldSchemaType("TIME")
  def time = Time

  val DateTime = TableFieldSchemaType("DATETIME")
  def dateTime = DateTime

  val Geography = TableFieldSchemaType("GEOGRAPHY")
  def geography = Geography

  val Numeric = TableFieldSchemaType("NUMERIC")
  def numeric = Numeric

  val BigNumeric = TableFieldSchemaType("BIGNUMERIC")
  def bigNumeric = BigNumeric

  val Record = TableFieldSchemaType("RECORD")
  def record = Record

  implicit val format: JsonFormat[TableFieldSchemaType] = StringEnum.jsonFormat(apply)
}

final case class TableFieldSchemaMode private (value: String) extends StringEnum
object TableFieldSchemaMode {

  /**
   * Java API
   */
  def create(value: String) = TableFieldSchemaMode(value)

  val Nullable = TableFieldSchemaMode("NULLABLE")
  def nullable = Nullable

  val Required = TableFieldSchemaMode("REQUIRED")
  def required = Required

  val Repeated = TableFieldSchemaMode("REPEATED")
  def repeated = Repeated

  implicit val format: JsonFormat[TableFieldSchemaMode] = StringEnum.jsonFormat(apply)
}

/**
 * TableListResponse model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list#response-body BigQuery reference]]
 *
 * @param nextPageToken a token to request the next page of results
 * @param tables tables in the requested dataset
 * @param totalItems the total number of tables in the dataset
 */
final case class TableListResponse private (nextPageToken: Option[String],
                                            tables: Option[Seq[Table]],
                                            totalItems: Option[Int]) {

  def getNextPageToken = nextPageToken.asJava
  def getTables = tables.map(_.asJava).asJava
  def getTotalItems = totalItems.asPrimitive

  def withNextPageToken(nextPageToken: util.Optional[String]) =
    copy(nextPageToken = nextPageToken.asScala)
  def withTables(tables: util.Optional[util.List[Table]]) =
    copy(tables = tables.asScala.map(_.asScala.toList))
  def withTotalItems(totalItems: util.OptionalInt) =
    copy(totalItems = totalItems.asScala)
}

object TableListResponse {

  /**
   * Java API: TableListResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list#response-body BigQuery reference]]
   *
   * @param nextPageToken a token to request the next page of results
   * @param tables tables in the requested dataset
   * @param totalItems the total number of tables in the dataset
   * @return a [[TableListResponse]]
   */
  def createTableListResponse(nextPageToken: util.Optional[String],
                              tables: util.Optional[util.List[Table]],
                              totalItems: util.OptionalInt) =
    TableListResponse(nextPageToken.asScala, tables.asScala.map(_.asScala.toList), totalItems.asScala)

  implicit val format: RootJsonFormat[TableListResponse] = jsonFormat3(apply)
  implicit val paginated: Paginated[TableListResponse] = _.nextPageToken
}
