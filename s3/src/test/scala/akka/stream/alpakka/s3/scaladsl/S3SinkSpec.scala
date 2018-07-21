/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.stream.alpakka.s3.{MemoryBufferType, Proxy, S3Settings}
import akka.stream.alpakka.s3.impl.{ListBucketVersion2, S3Headers, ServerSideEncryption}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider

class S3SinkSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  val awsCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials("my-AWS-access-key-ID", "my-AWS-password")
  )
  val regionProvider =
    new AwsRegionProvider {
      def getRegion: String = "us-east-1"
    }
  val proxy = Option(Proxy("localhost", port, "http"))
  val settings =
    new S3Settings(MemoryBufferType, proxy, awsCredentialsProvider, regionProvider, false, None, ListBucketVersion2)
  val s3Client = new S3Client(settings)(system, materializer)

  it should "succeed uploading an empty file" in {
    mockUpload(expectedBody = "")

    //#upload
    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = s3Client.multipartUpload(bucket, bucketKey)
    //#upload

    val src = Source.empty[ByteString]

    val result: Future[MultipartUploadResult] = src.runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "upload a stream of bytes to S3" in {

    mockUpload()

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = s3Client.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "upload a stream of bytes to S3 with custom headers" in {

    mockUpload()

    //#upload
    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] =
      s3Client.multipartUploadWithHeaders(bucket, bucketKey, s3Headers = Some(S3Headers(ServerSideEncryption.AES256)))
    //#upload

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = Source
      .single(ByteString("some contents"))
      .runWith(s3Client.multipartUpload("nonexisting_bucket", "nonexisting_file.xml"))

    result.failed.futureValue.getMessage shouldBe "No key found"
  }

  it should "copy a file from source bucket to target bucket when expected content length is less then chunk size" in {
    mockCopy()

    //#multipart-copy
    val result: Future[MultipartUploadResult] = s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey)
    //#multipart-copy

    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket when expected content length is equal to chunk size" in {
    mockCopy(S3Client.MinChunkSize)

    val result = s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey)
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy an empty file from source bucket to target bucket" in {
    mockCopy(expectedContentLength = 0)

    val result = s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey)
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with SSE" in {
    mockCopySSE()

    val result = s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey, sse = Some(sseCustomerKeys))
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with custom header" in {
    mockCopy()

    val result =
      s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey, sse = Some(ServerSideEncryption.AES256))
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket when expected content length is greater then chunk size" in {
    mockCopyMulti()

    val result = s3Client.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey)
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with source version id provided" in {
    mockCopyVersioned()

    //#multipart-copy-with-source-version
    val result: Future[MultipartUploadResult] =
      s3Client.multipartCopy(bucket,
                             bucketKey,
                             targetBucket,
                             targetBucketKey,
                             sourceVersionId = Some("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"))
    //#multipart-copy-with-source-version

    result.futureValue shouldBe MultipartUploadResult(targetUrl,
                                                      targetBucket,
                                                      targetBucketKey,
                                                      etag,
                                                      Some("43jfkodU8493jnFJD9fjj3HHNVfdsQUIFDNsidf038jfdsjGFDSIRp"))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
