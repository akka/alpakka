/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.S3Exception
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.scaladsl.S3Client
import akka.stream.scaladsl.Sink
import com.github.tomakehurst.wiremock.client.WireMock._

import scala.concurrent.Await
import scala.concurrent.duration._

class S3SourceSpec extends WireMockBase {
  implicit val mat = ActorMaterializer()

  "S3Source" should "work in a happy case" in {
    val body = "<response>Some content</response>"
    mock
      .register(get(urlEqualTo("/testKey")).willReturn(aResponse().withStatus(200).withHeader("ETag", "fba9dede5f27731c9771645a39863328").withBody(body)))

    val result = new S3Client(AWSCredentials("", ""),
      "us-east-1").download("testBucket", "testKey").map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe body
  }

  it should "fail if request returns 404" in {
    val result = new S3Client(AWSCredentials("", ""),
      "us-east-1").download("sometest4398673", "30000184.xml").map(_.decodeString("utf8")).runWith(Sink.head)
    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("NoSuchKey")
    }
  }
}
