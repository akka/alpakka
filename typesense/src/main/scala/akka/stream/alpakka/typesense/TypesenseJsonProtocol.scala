package akka.stream.alpakka.typesense

import spray.json.DefaultJsonProtocol

import java.time.Instant

private[typesense] object TypesenseJsonProtocol extends DefaultJsonProtocol {
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

  implicit val fieldFormat: RootJsonFormat[Field] =
    jsonFormat(Field, "name", "type", "optional", "facet", "index")

  implicit val collectionSchemaFormat: RootJsonFormat[CollectionSchema] =
    jsonFormat(CollectionSchema, "name", "fields", "token_separators", "symbols_to_index", "default_sorting_field")

  implicit val fieldResponseFormat: RootJsonFormat[FieldResponse] =
    jsonFormat(FieldResponse, "name", "type", "optional", "facet", "index")

  implicit val collectionResponseFormat: RootJsonFormat[CollectionResponse] = {
    implicit val timestampFormat: RootJsonFormat[Instant] = new RootJsonFormat[Instant] {
      override def read(json: JsValue): Instant = json match {
        case JsNumber(value) => Instant.ofEpochSecond(value.toLongExact)
        case _ => deserializationError("Instant expected")
      }

      override def write(time: Instant): JsValue = JsNumber(time.getEpochSecond)
    }
    jsonFormat(CollectionResponse, "name", "num_documents", "fields", "default_sorting_field", "created_at")
  }
}
