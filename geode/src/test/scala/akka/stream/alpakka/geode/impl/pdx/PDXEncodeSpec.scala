/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl.pdx

import java.util.{Date, UUID}

import org.scalatest.{Matchers, WordSpec}

class PDXEncodeSpec extends WordSpec with Matchers {

  "PDXEncoder" should {

    "provides encoder for primitive types" in {
      PdxEncoder[Boolean]
      PdxEncoder[Int]
      PdxEncoder[List[Int]]
      PdxEncoder[Array[Int]]
      PdxEncoder[Double]
      PdxEncoder[List[Double]]
      PdxEncoder[Array[Double]]
      PdxEncoder[Float]
      PdxEncoder[List[Float]]
      PdxEncoder[Array[Float]]
      PdxEncoder[Long]
      PdxEncoder[Char]
      PdxEncoder[String]

    }

    "provides encoder for basic types" in {
      PdxEncoder[Date]
      PdxEncoder[UUID]
    }
  }
}
