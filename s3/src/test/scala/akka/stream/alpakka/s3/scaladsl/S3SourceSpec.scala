/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.stream.alpakka.s3.S3Exception
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

class S3SourceSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  "S3Source" should "download a stream of bytes from S3" in {

    mockDownload()

    //#download
    val s3Source: Source[ByteString, NotUsed] = s3Client.download("testBucket", "testKey")
    //#download

    val result: Future[String] = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe body
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = s3Client
      .download("nonexisting_bucket", "nonexisting_file.xml")
      .map(_.utf8String)
      .runWith(Sink.head)

    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("NoSuchKey")
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
