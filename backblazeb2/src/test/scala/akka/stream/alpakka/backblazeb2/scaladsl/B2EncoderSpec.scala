/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.stream.alpakka.backblazeb2.B2Encoder
import io.circe.generic.auto._
import io.circe.parser._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.io.Source

/** Tests that B2Encoder works as described in https://www.backblaze.com/b2/docs/string_encoding.html */
class B2EncoderSpec extends FlatSpec {
  case class TestCase(
      fullyEncoded: String,
      minimallyEncoded: String,
      string: String
  )

  private def checkB2(x: TestCase) = {
    val encoded = B2Encoder.encode(x.string)
    encoded should (equal(x.fullyEncoded) or equal(x.minimallyEncoded))
    val decoded = B2Encoder.decode(encoded)
    decoded shouldEqual x.string
  }

  it should "work for all tests in url-encoding-tests.json" in {
    val data = Source.fromFile("backblazeb2/src/test/resources/url-encoding-tests.json").mkString
    val testCases = decode[List[TestCase]](data) getOrElse sys.error("Failed to parse test cases")
    testCases foreach checkB2
  }

  private def checkBase64Encode(value: String, expected: String) =
    B2Encoder.encodeBase64(value) shouldEqual expected

  it should "encode base 64" in { // from https://www.ietf.org/rfc/rfc4648.txt
    checkBase64Encode("", "")
    checkBase64Encode("f", "Zg==")
    checkBase64Encode("fo", "Zm8=")
    checkBase64Encode("foo", "Zm9v")
    checkBase64Encode("foob", "Zm9vYg==")
    checkBase64Encode("fooba", "Zm9vYmE=")
    checkBase64Encode("foobar", "Zm9vYmFy")
  }

  it should "encode sha1" in {
    B2Encoder.sha1String("this is just a test") shouldEqual "b9c999e4c3595750b70108c84dd8aa4599c54270"
  }
}
