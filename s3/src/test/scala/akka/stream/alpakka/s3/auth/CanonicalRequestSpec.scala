/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import org.scalatest.{FlatSpec, Matchers}

class CanonicalRequestSpec extends FlatSpec with Matchers {

  it should "correctly build a canonicalString for eu-central-1" in {
    val req = HttpRequest(
      HttpMethods.GET,
      Uri("https://s3-eu-central-1.amazonaws.com/my.test.bucket/test%20folder/test%20file%20(1).txt?uploads")
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
      Uri("https://mytestbucket.s3.amazonaws.com/test%20folder/test%20file%20(1).txt")
        .withQuery(Query("partNumber" -> "2", "uploadId" -> "testUploadId"))
    ).withHeaders(
      RawHeader("x-amz-content-sha256", "testhash"),
      `Content-Type`(ContentTypes.`application/json`)
    )
    val canonical = CanonicalRequest.from(req)
    canonical.canonicalString should equal(
      """GET
        |/test%20folder/test%20file%20%281%29.txt
        |partNumber=2&uploadId=testUploadId
        |content-type:application/json
        |x-amz-content-sha256:testhash
        |
        |content-type;x-amz-content-sha256
        |testhash""".stripMargin
    )
  }
}
