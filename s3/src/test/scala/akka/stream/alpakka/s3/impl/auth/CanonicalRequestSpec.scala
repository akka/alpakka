/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CanonicalRequestSpec extends AnyFlatSpec with Matchers {

  it should "correctly build a canonicalString for eu-central-1" in {
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://s3.eu-central-1.amazonaws.com/my.test.bucket/test%20folder/test%20file%20(1).txt?uploads")
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/my.test.bucket/test%20folder/test%20file%20%281%29.txt
        |uploads=
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }

  it should "correctly build a canonicalString for us-east-1" in {
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://mytestbucket.s3.us-east-1.amazonaws.com/test%20folder/test%20file%20(1):.txt")
        .withQuery(Query("partNumber" -> "2", "uploadId" -> "testUploadId"))
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/test%20folder/test%20file%20%281%29%3A.txt
        |partNumber=2&uploadId=testUploadId
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }

  it should "correctly build a canonicalString for us-east-1 with empty path" in {
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://mytestbucket.s3.us-east-1.amazonaws.com")
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/
        |
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }

  it should "correctly build a canonicalString with special characters in the path" in {
    // this corresponds with not encode path: /føldęrü/1234()[]><!? .TXT
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://mytestbucket.s3.us-east-1.amazonaws.com/f%C3%B8ld%C4%99r%C3%BC/1234()%5B%5D%3E%3C!%3F%20.TXT")
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/f%C3%B8ld%C4%99r%C3%BC/1234%28%29%5B%5D%3E%3C%21%3F%20.TXT
        |
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }

  // https://tools.ietf.org/html/rfc3986#section-2.3
  it should "correctly build a canonicalString for 'not unreserved' RFC 3986 characters in the query" in {
    val (name, expectedName) = ("abc#", "abc%" + '#'.toHexString.toUpperCase)
    val (value, expectedValue) = ("def(", "def%" + '('.toHexString.toUpperCase)

    val request =
      HttpRequest(
        HttpMethods.GET,
        Uri(s"https://mytestbucket.s3.us-east-1.amazonaws.com/test")
          .withQuery(Uri.Query(name.toString -> value.toString))
      ).withHeaders(
        RawHeader("x-amz-content-sha256", "testhash"),
        `Content-Type`(ContentTypes.`application/json`)
      )

    val canonicalRequest = CanonicalRequest.from(request)
    canonicalRequest.canonicalString should equal {
      s"""GET
        |/test
        |$expectedName=$expectedValue
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    }
  }

  // https://tools.ietf.org/html/rfc3986#section-2.3
  it should "correctly build a canonicalString for all RFC 3986 reserved characters except / in the path" in {
    val reservedCharacters = ":?#[]@!$&'()*+,;="
    reservedCharacters.foreach { char =>
      withClue(s"failed for path containing reserved character [$char]:") {
        val expectedCharEncoding = "%" + char.toHexString.toUpperCase

        val request =
          HttpRequest(
            HttpMethods.GET,
            Uri(s"https://mytestbucket.s3.us-east-1.amazonaws.com")
              .withPath(Uri.Path.Empty / s"file-$char.txt")
          ).withHeaders(
            RawHeader("x-amz-content-sha256", "testhash"),
            `Content-Type`(ContentTypes.`application/json`)
          )

        val canonicalRequest = CanonicalRequest.from(request)
        canonicalRequest.canonicalString should equal {
          s"""GET
            |/file-$expectedCharEncoding.txt
            |
            |content-type:application/json
            |x-amz-content-sha256:testhash
            |
            |content-type;x-amz-content-sha256
            |testhash""".stripMargin
        }
      }
    }
  }

  it should "correctly build a canonicalString when synthetic headers are present" in {
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://s3.eu-central-1.amazonaws.com/my.test.bucket/file+name.txt")
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`),
      `Raw-Request-URI`("/my.test.bucket/file%2Bname.txt"),
      `Remote-Address`(RemoteAddress.Unknown)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/my.test.bucket/file%2Bname.txt
        |
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }
}
