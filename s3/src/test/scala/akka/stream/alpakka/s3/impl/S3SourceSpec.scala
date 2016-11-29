/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.stream.alpakka.s3.S3Exception
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.scaladsl.S3Client
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

class S3SourceSpec extends WireMockBase() with Matchers with ScalaFutures {

  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system))

  "S3SourceSpec" should "work in a happy case" in {
    val body = "<response>Some content</response>"
    stubFor(get(urlEqualTo("/testKey")).willReturn(aResponse().withStatus(200).withHeader("ETag", "fba9dede5f27731c9771645a39863328").withBody(body)))

    val result = new S3Client(AWSCredentials("", ""),
      "us-east-1").download("testBucket", "testKey").map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe body
  }

  it should "fail if request return 404" in {
    val result = new S3Client(AWSCredentials("", ""),
      "us-east-1").download("sometest4398673", "30000184.xml").map(_.decodeString("utf8")).runWith(Sink.head)
    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("NoSuchKey")
    }
  }
}
