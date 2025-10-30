/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.geode.impl.pdx

import java.util.{Date, UUID}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PDXDecoderSpec extends AnyWordSpec with Matchers {

  "PDX decoder" should {
    "decode primitive type" in {
      PdxDecoder[Boolean]
      PdxDecoder[Int]
      PdxDecoder[List[Int]]
      PdxDecoder[Array[Int]]
      PdxDecoder[Long]
      PdxDecoder[List[Long]]
      PdxDecoder[Array[Long]]
      PdxDecoder[Float]
      PdxDecoder[List[Float]]
      PdxDecoder[Array[Float]]
      PdxDecoder[Double]
      PdxDecoder[List[Double]]
      PdxDecoder[Array[Double]]

      PdxDecoder[Char]
      PdxDecoder[List[Char]]
      PdxDecoder[Array[Char]]
      PdxDecoder[String]
      PdxDecoder[List[String]]
      PdxDecoder[Array[String]]

    }

    "decode basic types" in {
      PdxDecoder[Date]
      PdxDecoder[List[Date]]

      PdxDecoder[UUID]
      PdxDecoder[List[UUID]]

    }
  }

}
