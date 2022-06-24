package akka.stream.alpakka.typesense.json

import akka.stream.alpakka.typesense.{
  CollectionResponse,
  CollectionSchema,
  Field,
  FieldResponse,
  FieldType,
  TypesenseJsonProtocol
}
import spray.json._

import java.time.Instant

class CollectionTypesenseJsonProtocolSpec extends TypesenseJsonProtocolSpec {
  import TypesenseJsonProtocol._

  def fieldReponse(name: String): FieldResponse =
    FieldResponse(name = name, `type` = FieldType.String, optional = true, facet = false, index = true)

  def fieldResponseJson(name: String): JsObject = JsObject(
    "name" -> JsString(name),
    "type" -> JsString("string"),
    "optional" -> JsBoolean(true),
    "facet" -> JsBoolean(false),
    "index" -> JsBoolean(true)
  )

  describe("Field") {
    describe("should be serialized and deserialized") {
      it("with string field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.String),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string")
          )
        )
      }

      it("with string[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.StringArray),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string[]")
          )
        )
      }

      it("with int32 field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Int32),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("int32")
          )
        )
      }

      it("with int32[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Int32Array),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("int32[]")
          )
        )
      }

      it("with int64 field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Int64),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("int64")
          )
        )
      }

      it("with int64[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Int64Array),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("int64[]")
          )
        )
      }

      it("with float field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Float),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("float")
          )
        )
      }

      it("with float[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.FloatArray),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("float[]")
          )
        )
      }

      it("with bool field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Bool),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("bool")
          )
        )
      }

      it("with bool[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.BoolArray),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("bool[]")
          )
        )
      }

      it("with geopoint field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Geopoint),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("geopoint")
          )
        )
      }

      it("with geopoint[] field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.GeopointArray),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("geopoint[]")
          )
        )
      }

      it("with string* field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.StringAutoArray),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string*")
          )
        )
      }

      it("with auto field type") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.Auto),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("auto")
          )
        )
      }

      describe("with optional flag") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.String, optional = Some(true)),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string"),
            "optional" -> JsBoolean(true)
          )
        )
      }

      describe("with facet flag") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.String, facet = Some(true)),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string"),
            "facet" -> JsBoolean(true)
          )
        )
      }

      describe("with index flag") {
        checkJson(
          data = Field(name = "company_name", `type` = FieldType.String, index = Some(true)),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string"),
            "index" -> JsBoolean(true)
          )
        )
      }
    }
  }

  describe("CollectionSchema") {
    describe("should be serialized and deserialized") {
      it("with only required fields") {
        checkJson(
          data = CollectionSchema(name = "companies", fields = Seq(Field("company_name", FieldType.String))),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            )
          )
        )
      }

      it("with specified non empty token separators") {
        checkJson(
          data = CollectionSchema(name = "companies",
                                  fields = Seq(Field("company_name", FieldType.String)),
                                  tokenSeparators = Some(Seq("-"))),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            ),
            "token_separators" -> JsArray(JsString("-"))
          )
        )
      }

      it("with specified empty token separators") {
        checkJson(
          data = CollectionSchema(name = "companies",
                                  fields = Seq(Field("company_name", FieldType.String)),
                                  tokenSeparators = Some(Seq.empty)),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            ),
            "token_separators" -> JsArray()
          )
        )
      }

      it("with specified non empty symbols to index") {
        checkJson(
          data = CollectionSchema(name = "companies",
                                  fields = Seq(Field("company_name", FieldType.String)),
                                  symbolsToIndex = Some(Seq("+"))),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            ),
            "symbols_to_index" -> JsArray(JsString("+"))
          )
        )
      }

      it("with specified empty symbols to index") {
        checkJson(
          data = CollectionSchema(name = "companies",
                                  fields = Seq(Field("company_name", FieldType.String)),
                                  symbolsToIndex = Some(Seq.empty)),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            ),
            "symbols_to_index" -> JsArray()
          )
        )
      }

      it("with specified default sorting field") {
        checkJson(
          data = CollectionSchema(name = "companies",
                                  fields = Seq(Field("company_name", FieldType.String)),
                                  defaultSortingField = Some("company_nr")),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            ),
            "default_sorting_field" -> JsString("company_nr")
          )
        )
      }
    }
  }

  describe("FieldResponse") {
    describe("should be serialized and deserialized") {
      it("with all fields") {
        checkJson(
          data = fieldReponse("company_name"),
          expectedJson = fieldResponseJson("company_name")
        )
      }
    }
  }

  describe("CollectionResponse") {
    describe("should be serialized and deserialized") {
      it("with all fields") {
        checkJson(
          data = CollectionResponse(
            name = "companies",
            numDocuments = 10,
            fields = Seq(fieldReponse("company_name")),
            defaultSortingField = "company_nr",
            createdAt = Instant.ofEpochSecond(1656141317)
          ),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "num_documents" -> JsNumber(10),
            "fields" -> JsArray(fieldResponseJson("company_name")),
            "default_sorting_field" -> JsString("company_nr"),
            "created_at" -> JsNumber(1656141317)
          )
        )
      }
    }
  }
}
