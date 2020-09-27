/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import org.scalatest.wordspec.AnyWordSpec

class EsParamsSpec extends AnyWordSpec {
  "EsParams" should {
    "not allow setting a null indexName" in {
      assertThrows[IllegalArgumentException] {
        EsParams().withIndexName(null)
      }
    }

    "not allow setting a null typeName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        EsParams().withApiVersion(ApiVersion.V5).withTypeName(null)
      }
    }

    "allow setting a null typeName for API version V7" in {
      val result = EsParams().withApiVersion(ApiVersion.V7).withTypeName(null)
      assert(result.typeName.isEmpty)
    }

    "not allow setting an empty typeName for API version V5" in {
      assertThrows[IllegalArgumentException] {
        EsParams().withApiVersion(ApiVersion.V5).withTypeName("")
      }
    }

    "allow setting an empty typeName for API version V7" in {
      val result = EsParams().withApiVersion(ApiVersion.V7).withTypeName("")
      assert(result.typeName.contains(""))
    }
  }
}
