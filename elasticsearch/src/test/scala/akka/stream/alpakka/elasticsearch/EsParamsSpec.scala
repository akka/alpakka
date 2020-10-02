/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import org.scalatest.wordspec.AnyWordSpec

class EsParamsSpec extends AnyWordSpec {
  "EsParams" should {
    "not allow setting a null indexName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        EsParams.V5(null, "_doc")
      }
    }

    "not allow setting a null typeName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        EsParams.V5("index", null)
      }
    }

    "not allow setting a null indexName for API version V7" in {
      assertThrows[IllegalArgumentException] {
        EsParams.V7(null)
      }
    }
  }
}
