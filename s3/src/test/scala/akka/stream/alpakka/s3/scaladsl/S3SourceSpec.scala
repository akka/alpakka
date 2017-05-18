/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.{MemoryBufferType, Proxy, S3Exception, S3Settings}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

class S3SourceSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  //#client
  val awsCredentials = AWSCredentials(accessKeyId = "my-AWS-access-key-ID", secretAccessKey = "my-AWS-password")
  val proxy = Option(Proxy("localhost", port, "http"))
  val settings = new S3Settings(MemoryBufferType, "", proxy, awsCredentials, "us-east-1", false)
  val s3Client = new S3Client(settings)(system, materializer)
  //#client

  "S3Source" should "download a stream of bytes from S3" in {

    mockDownload()

    //#download
    val s3Source: Source[ByteString, NotUsed] = s3Client.download(bucket, bucketKey)
    //#download

    val result: Future[String] = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe body
  }

  it should "download a range of file's bytes from S3 if bytes range given" in {

    mockRangedDownload()

    //#rangedDownload
    val s3Source: Source[ByteString, NotUsed] =
      s3Client.download(bucket, bucketKey, ByteRange(bytesRangeStart, bytesRangeEnd))
    //#rangedDownload

    val result: Future[Array[Byte]] = s3Source.map(_.toArray).runWith(Sink.head)

    result.futureValue shouldBe rangeOfBody
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
