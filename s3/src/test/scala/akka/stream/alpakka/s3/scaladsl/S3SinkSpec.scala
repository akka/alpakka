/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.stream.alpakka.s3.impl.{S3Headers, S3Stream, ServerSideEncryption}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

class S3SinkSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  it should "succeed uploading an empty file" in {
    mockUpload(expectedBody = "")

    //#upload
    val s3Sink: Sink[ByteString, Source[MultipartUploadResult, NotUsed]] = S3.multipartUpload(bucket, bucketKey)
    //#upload

    val src = Source.empty[ByteString]

    val result: Source[MultipartUploadResult, NotUsed] = src.runWith(s3Sink)

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "upload a stream of bytes to S3" in {

    mockUpload()

    val s3Sink: Sink[ByteString, Source[MultipartUploadResult, NotUsed]] = S3.multipartUpload(bucket, bucketKey)

    val result: Source[MultipartUploadResult, NotUsed] = Source.single(ByteString(body)).runWith(s3Sink)

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry upload after internal server error" in {

    mockUploadWithInternalError(body)

    val s3Sink: Sink[ByteString, Source[MultipartUploadResult, NotUsed]] = S3.multipartUpload(bucket, bucketKey)

    val result: Source[MultipartUploadResult, NotUsed] = Source.single(ByteString(body)).runWith(s3Sink)

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "upload a stream of bytes to S3 with custom headers" in {

    mockUpload()

    //#upload
    val s3Sink: Sink[ByteString, Source[MultipartUploadResult, NotUsed]] =
      S3.multipartUploadWithHeaders(bucket, bucketKey, s3Headers = Some(S3Headers(ServerSideEncryption.AES256)))
    //#upload

    val result: Source[MultipartUploadResult, NotUsed] = Source.single(ByteString(body)).runWith(s3Sink)

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = Source
      .single(ByteString("some contents"))
      .runWith(S3.multipartUpload("nonexisting_bucket", "nonexisting_file.xml"))
      .runWith(Sink.head)

    result.failed.futureValue.getMessage shouldBe "No key found"
  }

  it should "copy a file from source bucket to target bucket when expected content length is less then chunk size" in {
    mockCopy()

    //#multipart-copy
    val result: Source[MultipartUploadResult, NotUsed] =
      S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    //#multipart-copy

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy a file from source bucket to target bucket when expected content length is equal to chunk size" in {
    mockCopy(S3Stream.MinChunkSize)

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy an empty file from source bucket to target bucket" in {
    mockCopy(expectedContentLength = 0)

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy a file from source bucket to target bucket with SSE" in {
    mockCopySSE()

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey, sse = Some(sseCustomerKeys)).run()
    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy a file from source bucket to target bucket with custom header" in {
    mockCopy()

    val result =
      S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey, sse = Some(ServerSideEncryption.AES256)).run()
    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy a file from source bucket to target bucket when expected content length is greater then chunk size" in {
    mockCopyMulti()

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(targetUrl,
                                                                         targetBucket,
                                                                         targetBucketKey,
                                                                         etag,
                                                                         None)
  }

  it should "copy a file from source bucket to target bucket with source version id provided" in {
    mockCopyVersioned()

    //#multipart-copy-with-source-version
    val result: Source[MultipartUploadResult, NotUsed] =
      S3.multipartCopy(bucket,
                       bucketKey,
                       targetBucket,
                       targetBucketKey,
                       sourceVersionId = Some("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"))
        .run()
    //#multipart-copy-with-source-version

    result.runWith(Sink.head).futureValue shouldBe MultipartUploadResult(
      targetUrl,
      targetBucket,
      targetBucketKey,
      etag,
      Some("43jfkodU8493jnFJD9fjj3HHNVfdsQUIFDNsidf038jfdsjGFDSIRp")
    )
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
