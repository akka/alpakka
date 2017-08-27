/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.{MemoryBufferType, Proxy, S3Settings}
import akka.stream.alpakka.s3.impl.{S3Headers, ServerSideEncryption}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

class S3SinkSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  val awsCredentials = AWSCredentials(accessKeyId = "my-AWS-access-key-ID", secretAccessKey = "my-AWS-password")
  val proxy = Option(Proxy("localhost", port, "http"))
  val settings = new S3Settings(MemoryBufferType, "", proxy, awsCredentials, "us-east-1", false)
  val s3Client = new S3Client(settings)(system, materializer)

  "S3Sink" should "upload a stream of bytes to S3" in {

    mockUpload()

    //#upload
    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = s3Client.multipartUpload(bucket, bucketKey)
    //#upload

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag)
  }

  it should "upload a stream of bytes to S3 with custom headers" in {

    mockUpload()

    //#upload
    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] =
      s3Client.multipartUploadWithHeaders(bucket, bucketKey, s3Headers = Some(S3Headers(ServerSideEncryption.AES256)))
    //#upload

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag)
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = Source
      .single(ByteString("some contents"))
      .runWith(s3Client.multipartUpload("nonexisting_bucket", "nonexisting_file.xml"))

    result.failed.futureValue.getMessage should startWith("Can't initiate upload:")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
