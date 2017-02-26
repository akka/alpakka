/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import io.circe.parser._
import io.circe.generic.auto._
import cats.syntax.either._
import scala.io.Source

/** Tests that B2Encoder works as described in https://www.backblaze.com/b2/docs/string_encoding.html */
class B2EncoderSpec extends FlatSpec {
  case class TestCase(
      fullyEncoded: String,
      minimallyEncoded: String,
      string: String
  )

  private def check(x: TestCase) = {
    val encoded = B2Encoder.encode(x.string)
    encoded should (equal(x.fullyEncoded) or equal(x.minimallyEncoded))
    val decoded = B2Encoder.decode(encoded)
    decoded shouldEqual x.string
  }

  it should "work for all tests in url-encoding-tests.json" in {
    val data = Source.fromFile("backblazeb2/src/test/resources/url-encoding-tests.json").mkString
    val testCases = decode[List[TestCase]](data) getOrElse sys.error("Failed to parse test cases")
    testCases foreach check
  }
}
