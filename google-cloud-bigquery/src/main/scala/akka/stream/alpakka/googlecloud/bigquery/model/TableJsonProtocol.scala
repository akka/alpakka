/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.{deserializationError, JsString, JsValue, JsonFormat, RootJsonFormat}

import java.util
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

object TableJsonProtocol {

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
  final case class Table(tableReference: TableReference,
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
    def withLabels(labels: util.Optional[util.Map[String, String]]) =
      copy(labels = labels.asScala.map(_.asScala.toMap))
    def withSchema(schema: util.Optional[TableSchema]) =
      copy(schema = schema.asScala)
    def withNumRows(numRows: util.OptionalLong) =
      copy(numRows = numRows.asScala)
    def withLocation(location: util.Optional[String]) =
      copy(location = location.asScala)
  }

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
  def createTable(tableReference: TableReference,
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

  /**
   * TableReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference BigQuery reference]]
   *
   * @param projectId the ID of the project containing this table
   * @param datasetId the ID of the dataset containing this table
   * @param tableId the ID of the table
   */
  final case class TableReference(projectId: Option[String], datasetId: String, tableId: String) {

    def getProjectId = projectId.asJava
    def getDatasetId = datasetId
    def getTableId = tableId

    def withProjectId(projectId: util.Optional[String]) =
      copy(projectId = projectId.asScala)
    def withDatasetId(datasetId: String) =
      copy(datasetId = datasetId)
    def withTableId(tableId: String) =
      copy(tableId = tableId)
  }

  /**
   * Java API: TableReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference BigQuery reference]]
   *
   * @param projectId the ID of the project containing this table
   * @param datasetId the ID of the dataset containing this table
   * @param tableId the ID of the table
   * @return a [[TableReference]]
   */
  def createTableReference(projectId: util.Optional[String], datasetId: String, tableId: String) =
    TableReference(projectId.asScala, datasetId, tableId)

  /**
   * Schema of a table
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
   *
   * @param fields describes the fields in a table
   */
  final case class TableSchema(fields: Seq[TableFieldSchema]) {

    @silent("never used")
    @JsonCreator
    private def this(@JsonProperty(value = "fields", required = true) fields: util.List[TableFieldSchema]) =
      this(fields.asScala.toList)

    def getFields = fields.asJava
    def withFields(fields: util.List[TableFieldSchema]) = copy(fields = fields.asScala.toList)
  }

  /**
   * Java API: Schema of a table
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
   *
   * @param fields describes the fields in a table
   * @return a [[TableSchema]]
   */
  def createTableSchema(fields: util.List[TableFieldSchema]) = TableSchema(fields.asScala.toList)

  /**
   * Java API: Schema of a table
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema BigQuery reference]]
   *
   * @param fields describes the fields in a table
   * @return a [[TableSchema]]
   */
  @varargs
  def createTableSchema(fields: TableFieldSchema*) = TableSchema(fields.toList)

  /**
   * A field in TableSchema
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema BigQuery reference]]
   *
   * @param name the field name
   * @param `type` the field data type
   * @param mode the field mode
   * @param fields describes the nested schema fields if the type property is set to `RECORD`
   */
  final case class TableFieldSchema(name: String,
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
        Option(mode).map(TableFieldSchemaMode),
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
    def withMode(mode: util.Optional[TableFieldSchemaMode]) =
      copy(mode = mode.asScala)
    def withFields(fields: util.Optional[util.List[TableFieldSchema]]) =
      copy(fields = fields.asScala.map(_.asScala.toList))
  }

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
  def createTableFieldSchema(name: String,
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
  def createTableFieldSchema(name: String,
                             `type`: TableFieldSchemaType,
                             mode: util.Optional[TableFieldSchemaMode],
                             fields: TableFieldSchema*) =
    TableFieldSchema(name, `type`, mode.asScala, if (fields.nonEmpty) Some(fields.toList) else None)

  sealed case class TableFieldSchemaType(value: String) {
    def getValue = value
  }
  val StringType = TableFieldSchemaType("STRING")
  val BytesType = TableFieldSchemaType("BYTES")
  val IntegerType = TableFieldSchemaType("INTEGER")
  val FloatType = TableFieldSchemaType("FLOAT")
  val BooleanType = TableFieldSchemaType("BOOLEAN")
  val TimestampType = TableFieldSchemaType("TIMESTAMP")
  val DateType = TableFieldSchemaType("DATE")
  val TimeType = TableFieldSchemaType("TIME")
  val DateTimeType = TableFieldSchemaType("DATETIME")
  val GeographyType = TableFieldSchemaType("GEOGRAPHY")
  val NumericType = TableFieldSchemaType("NUMERIC")
  val RecordType = TableFieldSchemaType("RECORD")

  /**
   * Java API
   */
  def createTableFieldSchemaType(value: String) = TableFieldSchemaType(value)

  /** Java API */
  def stringType = StringType

  /** Java API */
  def bytesType = BytesType

  /** Java API */
  def integerType = IntegerType

  /** Java API */
  def floatType = FloatType

  /** Java API */
  def booleanType = BooleanType

  /** Java API */
  def timestampType = TimestampType

  /** Java API */
  def dateType = DateType

  /** Java API */
  def timeType = TimeType

  /** Java API */
  def dateTimeType = DateTimeType

  /** Java API */
  def geographyType = GeographyType

  /** Java API */
  def numericType = NumericType

  /** Java API */
  def recordType = RecordType

  sealed case class TableFieldSchemaMode(value: String) {
    def getValue = value
  }
  val NullableMode = TableFieldSchemaMode("NULLABLE")
  val RequiredMode = TableFieldSchemaMode("REQUIRED")
  val RepeatedMode = TableFieldSchemaMode("REPEATED")

  /**
   * Java API
   */
  def createTableFieldSchemaMode(value: String) = TableFieldSchemaMode(value)

  /** Java API */
  def nullableMode = NullableMode

  /** Java API */
  def requiredMode = RequiredMode

  /** Java API */
  def repeatedMode = RepeatedMode

  /**
   * TableListResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list#response-body BigQuery reference]]
   *
   * @param nextPageToken a token to request the next page of results
   * @param tables tables in the requested dataset
   * @param totalItems the total number of tables in the dataset
   */
  final case class TableListResponse(nextPageToken: Option[String],
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

  implicit val tableFieldSchemaTypeFormat: JsonFormat[TableFieldSchemaType] = new JsonFormat[TableFieldSchemaType] {
    override def read(json: JsValue): TableFieldSchemaType = json match {
      case JsString(x) => TableFieldSchemaType(x)
      case x => deserializationError("Expected TableFieldSchemaType as JsString, but got " + x)
    }
    override def write(obj: TableFieldSchemaType): JsValue = JsString(obj.value)
  }
  implicit val tableFieldSchemaModeFormat: JsonFormat[TableFieldSchemaMode] = new JsonFormat[TableFieldSchemaMode] {
    override def read(json: JsValue): TableFieldSchemaMode = json match {
      case JsString(x) => TableFieldSchemaMode(x)
      case x => deserializationError("Expected TableFieldSchemaMode as JsString, but got " + x)
    }
    override def write(obj: TableFieldSchemaMode): JsValue = JsString(obj.value)
  }
  implicit val fieldSchemaFormat: JsonFormat[TableFieldSchema] = lazyFormat(
    jsonFormat(TableFieldSchema, "name", "type", "mode", "fields")
  )
  implicit val schemaFormat: JsonFormat[TableSchema] = jsonFormat1(TableSchema)
  implicit val referenceFormat: JsonFormat[TableReference] = jsonFormat3(TableReference)
  implicit val format: RootJsonFormat[Table] = jsonFormat5(Table)
  implicit val listResponseFormat: RootJsonFormat[TableListResponse] = jsonFormat3(TableListResponse)
  implicit val paginated: Paginated[TableListResponse] = _.nextPageToken
}
