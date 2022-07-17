/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.json

import akka.stream.alpakka.typesense.impl.CollectionResponses.IndexManyDocumentsResponse
import spray.json._

class DocumentTypesenseJsonProtocolSpec extends TypesenseJsonProtocolSpec {
  import akka.stream.alpakka.typesense.impl.TypesenseJsonProtocol._

  describe("Index many documents response") {
    describe("should be serialized and deserialized") {
      it("for success") {
        checkJson(
          data = IndexManyDocumentsResponse(success = true, error = None, document = None),
          expectedJson = JsObject(
            "success" -> JsBoolean(true)
          )
        )
      }

      it("for failure") {
        checkJson(
          data = IndexManyDocumentsResponse(success = false,
                                            error = Some("Some error"),
                                            document = Some("Document details")),
          expectedJson = JsObject(
            "success" -> JsBoolean(false),
            "error" -> JsString("Some error"),
            "document" -> JsString("Document details")
          )
        )
      }
    }
  }
}
