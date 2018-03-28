/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.impl.{ListBucketVersion2, ServerSideEncryption}
import akka.stream.alpakka.s3.{MemoryBufferType, Proxy, S3Exception, S3Settings}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider

class S3SourceSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  //#client
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
  //#client

  "S3Source" should "download a stream of bytes from S3" in {

    mockDownload()

    //#download
    val (s3Source: Source[ByteString, _], _) = s3Client.download(bucket, bucketKey)
    //#download

    val result: Future[String] = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe body
  }

  "S3Source" should "download a metadata from S3" in {

    mockHead()

    val metadata = s3Client.getObjectMetadata(bucket, bucketKey)

    val Some(result) = metadata.futureValue

    result.eTag shouldBe Some(etag)
    result.contentLength shouldBe 8

  }

  it should "download a metadata from S3 using server side encryption" in {

    mockHeadSSEC()

    //#objectMetadata
    val metadata = s3Client.getObjectMetadata(bucket, bucketKey, Some(sseCustomerKeys))
    //#objectMetadata

    val Some(result) = metadata.futureValue

    result.eTag shouldBe Some(etagSSE)
    result.contentLength shouldBe 8
  }

  it should "download a range of file's bytes from S3 if bytes range given" in {

    mockRangedDownload()

    //#rangedDownload
    val (s3Source: Source[ByteString, _], _) =
      s3Client.download(bucket, bucketKey, Some(ByteRange(bytesRangeStart, bytesRangeEnd)))
    //#rangedDownload

    val result: Future[Array[Byte]] = s3Source.map(_.toArray).runWith(Sink.head)

    result.futureValue shouldBe rangeOfBody
  }

  it should "download a stream of bytes using customer server side encryption" in {

    mockDownloadSSEC()

    //#download
    val (s3Source, _) = s3Client.download(bucket, bucketKey, sse = Some(sseCustomerKeys))
    //#download

    val result = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe bodySSE
  }

  it should "fail if request returns 404" in {

    mock404s()

    val result = s3Client
      .download("nonexisting_bucket", "nonexisting_file.xml")
      ._1
      .map(_.utf8String)
      .runWith(Sink.head)

    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("NoSuchKey")
    }
  }

  it should "fail if download using server side encryption returns 'Invalid Request'" in {

    mockSSEInvalidRequest()

    val sse = ServerSideEncryption.CustomerKeys("encoded-key", Some("md5-encoded-key"))
    val result = s3Client
      .download(bucket, bucketKey, sse = Some(sse))
      ._1
      .map(_.utf8String)
      .runWith(Sink.head)

    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("InvalidRequest")
    }
  }

  it should "list keys for a given bucket with a prefix" in {
    mockListBucket()

    //#list-bucket
    val keySource: Source[ListBucketResultContents, NotUsed] = s3Client.listBucket(bucket, Some(listPrefix))
    //#list-bucket

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  it should "list keys for a given bucket with a prefix using the version 1 api" in {
    mockListBucketVersion1()

    val keySource: Source[ListBucketResultContents, NotUsed] =
      s3Client.listBucket(bucket, Some(listPrefix))

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
