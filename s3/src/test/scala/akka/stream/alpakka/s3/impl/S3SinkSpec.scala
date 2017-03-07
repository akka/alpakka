/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.S3Exception
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.scaladsl.{MultipartUploadResult, S3Client}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.github.tomakehurst.wiremock.client.WireMock._

import scala.concurrent.Await
import scala.concurrent.duration._

class S3SinkSpec extends WireMockBase {

  implicit val materializer = ActorMaterializer()

  "S3Sink" should "work in a happy case" in {
    val body = "<response>Some content</response>"
    val key = "testKey"
    val bucket = "testBucket"
    val uploadId = "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
    val etag = "5b27a21a97fcf8a7004dd1d906e7a5ba"
    val url = s"http://testbucket.s3.amazonaws.com/testKey"
    mock
      .register(post(urlEqualTo(s"/$bucket/$key?uploads")).willReturn(aResponse().withStatus(200).withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==").withHeader("x-amz-request-id", "656c76696e6727732072657175657374").withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                    |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    |  <Bucket>$bucket</Bucket>
                    |  <Key>$key</Key>
                    |  <UploadId>$uploadId</UploadId>
                    |</InitiateMultipartUploadResult>""".stripMargin)))

    mock.register(put(urlEqualTo(s"/$bucket/$key?partNumber=1&uploadId=$uploadId"))
        .withRequestBody(matching(body))
        .willReturn(aResponse().withStatus(200).withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt").withHeader("x-amz-request-id", "5A37448A37622243").withHeader("ETag", "\"" + etag + "\"")))

    mock.register(post(urlEqualTo(s"/$bucket/$key?uploadId=$uploadId"))
        .withRequestBody(containing("CompleteMultipartUpload"))
        .withRequestBody(containing(etag))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/xml; charset=UTF-8").withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt").withHeader("x-amz-request-id", "5A37448A3762224333").withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                    |<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    |  <Location>$url</Location>
                    |  <Bucket>$bucket</Bucket>
                    |  <Key>$key</Key>
                    |  <ETag>"$etag"</ETag>
                    |</CompleteMultipartUploadResult>""".stripMargin)))

    val result = Source(ByteString(body) :: Nil)
      .toMat(new S3Client(AWSCredentials("", ""), "us-east-1").multipartUpload(bucket, key))(Keep.right)
      .run

    Await.ready(result, 5.seconds).futureValue shouldBe MultipartUploadResult(url, bucket, key, etag)
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
