/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.CollectionResponses.IndexManyDocumentsResponse
import spray.json.DefaultJsonProtocol

import java.time.Instant

@InternalApi private[typesense] object TypesenseJsonProtocol extends DefaultJsonProtocol {
  import spray.json._

  implicit val fieldTypeFormat: RootJsonFormat[FieldType] = new RootJsonFormat[FieldType] {
    override def write(fieldType: FieldType): JsValue = fieldType match {
      case FieldType.String => JsString("string")
      case FieldType.StringArray => JsString("string[]")
      case FieldType.Int32 => JsString("int32")
      case FieldType.Int32Array => JsString("int32[]")
      case FieldType.Int64 => JsString("int64")
      case FieldType.Int64Array => JsString("int64[]")
      case FieldType.Float => JsString("float")
      case FieldType.FloatArray => JsString("float[]")
      case FieldType.Bool => JsString("bool")
      case FieldType.BoolArray => JsString("bool[]")
      case FieldType.Geopoint => JsString("geopoint")
      case FieldType.GeopointArray => JsString("geopoint[]")
      case FieldType.StringAutoArray => JsString("string*")
      case FieldType.Auto => JsString("auto")
    }

    override def read(json: JsValue): FieldType = json match {
      case JsString("string") => FieldType.String
      case JsString("string[]") => FieldType.StringArray
      case JsString("int32") => FieldType.Int32
      case JsString("int32[]") => FieldType.Int32Array
      case JsString("int64") => FieldType.Int64
      case JsString("int64[]") => FieldType.Int64Array
      case JsString("float") => FieldType.Float
      case JsString("float[]") => FieldType.FloatArray
      case JsString("bool") => FieldType.Bool
      case JsString("bool[]") => FieldType.BoolArray
      case JsString("geopoint") => FieldType.Geopoint
      case JsString("geopoint[]") => FieldType.GeopointArray
      case JsString("string*") => FieldType.StringAutoArray
      case JsString("auto") => FieldType.Auto
      case _ => deserializationError("FieldType expected")
    }
  }

  implicit val fieldFormat: RootJsonFormat[Field] = new RootJsonFormat[Field] {
    override def read(json: JsValue): Field = {
      val fields = json.asJsObject.fields
      Field(
        name = fields("name").convertTo[String],
        `type` = fields("type").convertTo[FieldType],
        optional = fields.get("optional").map(_.convertTo[Boolean]),
        facet = fields.get("facet").map(_.convertTo[Boolean]),
        index = fields.get("index").map(_.convertTo[Boolean])
      )
    }

    override def write(obj: Field): JsValue = JsObject(
      Seq(
        "name" -> obj.name.toJson,
        "type" -> obj.`type`.toJson
      )
      ++ obj.optional.map("optional" -> _.toJson)
      ++ obj.facet.map("facet" -> _.toJson)
      ++ obj.index.map("index" -> _.toJson): _*
    )
  }

  implicit val collectionSchemaFormat: RootJsonFormat[CollectionSchema] = new RootJsonFormat[CollectionSchema] {
    override def read(json: JsValue): CollectionSchema = {
      val fields = json.asJsObject.fields
      CollectionSchema(
        name = fields("name").convertTo[String],
        fields = fields("fields").convertTo[Seq[Field]],
        tokenSeparators = fields.get("token_separators").map(_.convertTo[Seq[String]]),
        symbolsToIndex = fields.get("symbols_to_index").map(_.convertTo[Seq[String]]),
        defaultSortingField = fields.get("default_sorting_field").map(_.convertTo[String])
      )
    }

    override def write(obj: CollectionSchema): JsValue = JsObject(
      Seq(
        "name" -> obj.name.toJson,
        "fields" -> obj.fields.toJson
      )
      ++ obj.tokenSeparators.map("token_separators" -> _.toJson)
      ++ obj.symbolsToIndex.map("symbols_to_index" -> _.toJson)
      ++ obj.defaultSortingField.map("default_sorting_field" -> _.toJson): _*
    )
  }

  implicit val fieldResponseFormat: RootJsonFormat[FieldResponse] = new RootJsonFormat[FieldResponse] {
    override def read(json: JsValue): FieldResponse = {
      val fields = json.asJsObject.fields
      FieldResponse(
        name = fields("name").convertTo[String],
        `type` = fields("type").convertTo[FieldType],
        facet = fields("facet").convertTo[Boolean],
        optional = fields.get("optional").map(_.convertTo[Boolean]),
        index = fields.get("index").map(_.convertTo[Boolean])
      )
    }

    override def write(obj: FieldResponse): JsValue = JsObject(
      Seq(
        "name" -> obj.name.toJson,
        "type" -> obj.`type`.toJson,
        "facet" -> obj.facet.toJson
      )
      ++ obj.optional.map("optional" -> _.toJson)
      ++ obj.index.map("index" -> _.toJson): _*
    )
  }

  implicit val collectionResponseFormat: RootJsonFormat[CollectionResponse] = new RootJsonFormat[CollectionResponse] {
    override def read(json: JsValue): CollectionResponse = {
      val fields = json.asJsObject.fields
      CollectionResponse(
        name = fields("name").convertTo[String],
        numDocuments = fields("num_documents").convertTo[Int],
        fields = fields("fields").convertTo[Seq[FieldResponse]],
        defaultSortingField = fields("default_sorting_field").convertTo[String],
        createdAt = Instant.ofEpochSecond(fields("created_at").convertTo[Long])
      )
    }

    override def write(obj: CollectionResponse): JsValue = JsObject(
      "name" -> obj.name.toJson,
      "num_documents" -> obj.numDocuments.toJson,
      "fields" -> obj.fields.toJson,
      "default_sorting_field" -> obj.defaultSortingField.toJson,
      "created_at" -> obj.createdAt.getEpochSecond.toJson
    )
  }

  implicit val indexManyDocumentsResponseFormat: RootJsonFormat[IndexManyDocumentsResponse] = jsonFormat3(
    IndexManyDocumentsResponse
  )
}
