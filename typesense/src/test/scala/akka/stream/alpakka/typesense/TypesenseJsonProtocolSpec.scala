package akka.stream.alpakka.typesense

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import spray.json.{RootJsonFormat, _}

class TypesenseJsonProtocolSpec extends AnyFunSpec with should.Matchers {
  import TypesenseJsonProtocol._

  describe("Field") {
    describe("should be serialized and deserialized") {
      it("if contains only required fields") {
        checkJson(
          data = Field("company_name", "string"),
          expectedJson = JsObject(
            "name" -> JsString("company_name"),
            "type" -> JsString("string")
          )
        )
      }
    }
  }

  describe("CollectionSchema") {
    describe("should be serialized and deserialized") {
      it("if contains only required fields") {
        checkJson(
          data = CollectionSchema(name = "companies", fields = Seq(Field("company_name", "string"))),
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
    }
  }

  describe("CollectionResponse") {
    describe("should be serialized and deserialized") {
      it("if contains only required fields") {
        checkJson(
          data =
            CollectionResponse(name = "companies", numDocuments = 10, fields = Seq(Field("company_name", "string"))),
          expectedJson = JsObject(
            "name" -> JsString("companies"),
            "num_documents" -> JsNumber(10),
            "fields" -> JsArray(
              JsObject(
                "name" -> JsString("company_name"),
                "type" -> JsString("string")
              )
            )
          )
        )
      }
    }
  }

  private def checkJson[T](data: T, expectedJson: JsObject)(implicit jsonFormat: RootJsonFormat[T]): Unit = {
    data.toJson shouldBe expectedJson
    expectedJson.convertTo[T] shouldBe data
  }
}
