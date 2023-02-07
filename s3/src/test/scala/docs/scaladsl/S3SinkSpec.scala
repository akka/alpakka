/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.file.Paths
import akka.NotUsed
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption}
import akka.stream.alpakka.s3.scaladsl.{S3, S3ClientIntegrationSpec, S3WireMockBase}
import akka.stream.alpakka.s3._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.github.tomakehurst.wiremock.client.WireMock.{headRequestedFor, postRequestedFor, putRequestedFor, urlEqualTo}
import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.matching.EqualToPattern
import org.scalatest.OptionValues
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.Future
import scala.concurrent.duration._

class S3SinkSpec extends S3WireMockBase with S3ClientIntegrationSpec with OptionValues {

  override protected def afterEach(): Unit =
    mock.removeMappings()

  it should "succeed uploading an empty file" in {
    mockUpload(expectedBody = "")

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucket, bucketKey)

    val src = Source.empty[ByteString]

    val result: Future[MultipartUploadResult] = src.runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "upload a stream of bytes to S3" in {

    mockUpload()

    //#upload
    val file: Source[ByteString, NotUsed] =
      Source.single(ByteString(body))

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] =
      S3.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] =
      file.runWith(s3Sink)
    //#upload

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry multipart upload initiation after a transient internal server error" in {

    mockMultipartUploadInitiationWithTransientError(body, Right(500))

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry multipart upload initiation after a transient downstream connection error" in {

    mockMultipartUploadInitiationWithTransientError(body, Left(Fault.CONNECTION_RESET_BY_PEER))

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry part upload after a transient internal server error" in {

    mockMultipartPartUploadWithTransient500Error(body)

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry part upload after a transient downstream connection error" in {

    mockMultipartPartUploadWithTransientConnectionError(body)

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucket, bucketKey)

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  "S3Sink" should "retry part upload no more than the configured number of times" in {

    mockMultipartPartUploadWithTransient500Error(body, 2)

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3
      .multipartUpload(bucket, bucketKey)
      .withAttributes(
        S3Attributes.settings(
          S3Settings().withMultipartUploadSettings(MultipartUploadSettings(RetrySettings(1, 0.seconds, 0.seconds, 0.0)))
        )
      )

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    val failure = intercept[TestFailedException] {
      result.futureValue
    }

    failure.cause.value.getClass shouldBe classOf[FailedUpload]
  }

  "S3Sink" should "be able to retry a disk-buffered part upload an arbitrary number of times" in {

    val numFailures = 5
    mockMultipartPartUploadWithTransient500Error(body, numFailures)

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] = S3
      .multipartUpload(bucket, bucketKey)
      .withAttributes(
        S3Attributes.settings(
          S3Settings()
            .withMultipartUploadSettings(MultipartUploadSettings(RetrySettings(numFailures, 0.seconds, 0.seconds, 0.0)))
            .withBufferType(DiskBufferType(Paths.get("")))
        )
      )

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "upload a stream of bytes to S3 with custom headers" in {

    mockUpload()

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] =
      S3.multipartUploadWithHeaders(bucket,
                                    bucketKey,
                                    s3Headers = S3Headers().withCannedAcl(CannedAcl.AuthenticatedRead))

    val result: Future[MultipartUploadResult] = Source.single(ByteString(body)).runWith(s3Sink)

    result.futureValue shouldBe MultipartUploadResult(url, bucket, bucketKey, etag, None)
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = Source
      .single(ByteString("some contents"))
      .runWith(S3.multipartUpload("nonexisting-bucket", "nonexisting_file.xml"))

    result.failed.futureValue.getMessage shouldBe "No key found"
  }

  it should "fail if part upload requests fail perpetually" in {

    mockUnrecoverableMultipartPartUploadFailure()

    val result = Source
      .single(ByteString(body))
      .runWith(S3.multipartUpload(bucket, bucketKey))

    val exception = result.failed.futureValue
    exception shouldBe a[FailedUpload]
    result.failed.futureValue.getMessage should startWith("Upload part 1 request failed")
  }

  it should "copy a file from source bucket to target bucket when expected content length is less then chunk size" in {
    mockCopy()

    //#multipart-copy
    val result: Future[MultipartUploadResult] =
      S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    //#multipart-copy

    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket when expected content length is equal to chunk size" in {
    mockCopyMinChunkSize()

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy an empty file from source bucket to target bucket" in {
    mockCopy(expectedContentLength = 0)

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with SSE" in {
    mockCopySSE()

    //#multipart-copy-sse
    val keys = ServerSideEncryption
      .customerKeys(sseCustomerKey)
      .withMd5(sseCustomerMd5Key)

    val result: Future[MultipartUploadResult] =
      S3.multipartCopy(bucket,
                       bucketKey,
                       targetBucket,
                       targetBucketKey,
                       s3Headers = S3Headers().withServerSideEncryption(keys))
        .run()
    //#multipart-copy-sse

    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with custom header" in {
    mockCopySSE()

    val requestPayerHeader = "x-amz-request-payer"
    val requestPayerHeaderValue = "requester"

    val keys = ServerSideEncryption
      .customerKeys(sseCustomerKey)
      .withMd5(sseCustomerMd5Key)
    val sseCAlgorithmHeader = "x-amz-server-side-encryption-customer-algorithm"
    val sseCAlgorithmHeaderValue = "AES256"
    val sseCKeyHeader = "x-amz-server-side-encryption-customer-key"
    val sseCKeyHeaderMd5 = "x-amz-server-side-encryption-customer-key-MD5"
    val sseCKeyHeaderMd5Value = "md5"
    val sseCKeyHeaderValue = sseCustomerKey
    val sseCSourceAlgorithmHeader = "x-amz-copy-source-server-side-encryption-customer-algorithm"
    val sseCSourceAlgorithmHeaderValue = "AES256"
    val sseCSourceKeyHeader = "x-amz-copy-source-server-side-encryption-customer-key"
    val sseCSourceKeyHeaderValue = sseCustomerKey

    val result =
      S3.multipartCopy(
          bucket,
          bucketKey,
          targetBucket,
          targetBucketKey,
          s3Headers = S3Headers()
            .withServerSideEncryption(keys)
            .withCustomHeaders(Map(requestPayerHeader -> requestPayerHeaderValue))
        )
        .run()

    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)

    mock verifyThat
    headRequestedFor(urlEqualTo(s"/$bucketKey"))
      .withHeader(sseCAlgorithmHeader, new EqualToPattern(sseCAlgorithmHeaderValue))
      .withHeader(sseCKeyHeader, new EqualToPattern(sseCKeyHeaderValue))
      .withHeader(requestPayerHeader, new EqualToPattern(requestPayerHeaderValue))

    mock verifyThat
    postRequestedFor(urlEqualTo(s"/$targetBucketKey?uploads"))
      .withHeader(sseCAlgorithmHeader, new EqualToPattern(sseCAlgorithmHeaderValue))
      .withHeader(sseCKeyHeader, new EqualToPattern(sseCKeyHeaderValue))
      .withHeader(requestPayerHeader, new EqualToPattern(requestPayerHeaderValue))

    mock verifyThat
    putRequestedFor(urlEqualTo(s"/$targetBucketKey?partNumber=1&uploadId=$uploadId"))
      .withHeader(sseCAlgorithmHeader, new EqualToPattern(sseCAlgorithmHeaderValue))
      .withHeader(sseCKeyHeader, new EqualToPattern(sseCKeyHeaderValue))
      .withHeader(sseCSourceAlgorithmHeader, new EqualToPattern(sseCSourceAlgorithmHeaderValue))
      .withHeader(sseCSourceKeyHeader, new EqualToPattern(sseCSourceKeyHeaderValue))
      .withHeader(requestPayerHeader, new EqualToPattern(requestPayerHeaderValue))

    // Only SSE headers possible, no other headers
    mock verifyThat
    postRequestedFor(urlEqualTo(s"/$targetBucketKey?uploadId=$uploadId"))
      .withHeader(sseCAlgorithmHeader, new EqualToPattern(sseCAlgorithmHeaderValue))
      .withHeader(sseCKeyHeader, new EqualToPattern(sseCKeyHeaderValue))
      .withHeader(sseCKeyHeaderMd5, new EqualToPattern(sseCKeyHeaderMd5Value))
      .withoutHeader(requestPayerHeader)

  }

  it should "copy a file from source bucket to target bucket when expected content length is greater then chunk size" in {
    mockCopyMulti()

    val result = S3.multipartCopy(bucket, bucketKey, targetBucket, targetBucketKey).run()
    result.futureValue shouldBe MultipartUploadResult(targetUrl, targetBucket, targetBucketKey, etag, None)
  }

  it should "copy a file from source bucket to target bucket with source version id provided" in {
    mockCopyVersioned()

    //#multipart-copy-with-source-version
    val result: Future[MultipartUploadResult] =
      S3.multipartCopy(bucket,
                       bucketKey,
                       targetBucket,
                       targetBucketKey,
                       sourceVersionId = Some("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"))
        .run()
    //#multipart-copy-with-source-version

    result.futureValue shouldBe MultipartUploadResult(
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
