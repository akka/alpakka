/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.elasticsearch

import org.scalatest.wordspec.AnyWordSpec

class ElasticsearchParamsSpec extends AnyWordSpec {
  "elasticsearchParams" should {
    "not allow setting a null indexName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchParams.V5(null, "_doc")
      }
    }

    "not allow setting a null typeName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchParams.V5("index", null)
      }
    }

    "not allow setting a null indexName for API version V7" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchParams.V7(null)
      }
    }
  }
}
