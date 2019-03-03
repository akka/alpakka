/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpResponse}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.headers.ServerSideEncryption
import akka.stream.alpakka.s3.scaladsl.{S3, S3ClientIntegrationSpec, S3WireMockBase}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.regions.AwsRegionProvider

import scala.concurrent.Future

class S3SourceSpec extends S3WireMockBase with S3ClientIntegrationSpec {

  override protected def afterEach(): Unit =
    mock.removeMappings()

  "S3Source" should "download a stream of bytes from S3" in {

    mockDownload()

    //#download
    val s3File: Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] =
      S3.download(bucket, bucketKey)

    val Some((data: Source[ByteString, _], metadata)) =
      s3File.runWith(Sink.head).futureValue

    val result: Future[String] =
      data.map(_.utf8String).runWith(Sink.head)
    //#download

    result.futureValue shouldBe body

    //#downloadToAkkaHttp
    HttpResponse(
      entity = HttpEntity(
        metadata.contentType
          .flatMap(ContentType.parse(_).right.toOption)
          .getOrElse(ContentTypes.`application/octet-stream`),
        metadata.contentLength,
        data
      )
    )
    //#downloadToAkkaHttp
  }

  "S3Source" should "use custom settings when downloading a file" in {

    val region = "my-custom-region"

    mockDownload(region)

    val customRegion = S3Ext(system).settings
      .withS3RegionProvider(new AwsRegionProvider {
        override def getRegion: String = region
      })

    val Some((data: Source[ByteString, _], _)) = S3
      .download(bucket, bucketKey)
      .withAttributes(S3Attributes.settings(customRegion))
      .runWith(Sink.head)
      .futureValue

    data.map(_.utf8String).runWith(Sink.head).futureValue shouldBe body
  }

  "S3Source" should "download a metadata from S3" in {

    val contentLength = 8
    mockHead(contentLength)

    //#objectMetadata
    val metadata: Source[Option[ObjectMetadata], NotUsed] =
      S3.getObjectMetadata(bucket, bucketKey)
    //#objectMetadata

    val Some(result) = metadata.runWith(Sink.head).futureValue

    result.eTag shouldBe Some(etag)
    result.contentLength shouldBe contentLength
    result.versionId shouldBe empty
  }

  "S3Source" should "download a metadata from S3 for a big file" in {

    val contentLength = Long.MaxValue
    mockHead(contentLength)

    val metadata = S3.getObjectMetadata(bucket, bucketKey)

    val Some(result) = metadata.runWith(Sink.head).futureValue

    result.eTag shouldBe Some(etag)
    result.contentLength shouldBe contentLength
    result.versionId shouldBe empty
  }

  "S3Source" should "download a metadata from S3 for specific version" in {

    val versionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"
    mockHeadWithVersion(versionId)

    val metadata = S3.getObjectMetadata(bucket, bucketKey, Some(versionId))

    val Some(result) = metadata.runWith(Sink.head).futureValue

    result.eTag shouldBe Some(etag)
    result.contentLength shouldBe 8
    result.versionId.fold(fail("unable to get versionId from S3")) { vId =>
      vId shouldEqual versionId
    }
  }

  it should "download a metadata from S3 using server side encryption" in {

    mockHeadSSEC()

    val metadata = S3.getObjectMetadata(bucket, bucketKey, sse = Some(sseCustomerKeys))

    val Some(result) = metadata.runWith(Sink.head).futureValue

    result.eTag shouldBe Some(etagSSE)
    result.contentLength shouldBe 8
  }

  it should "download a range of file's bytes from S3 if bytes range given" in {

    mockRangedDownload()

    //#rangedDownload
    val downloadResult = S3.download(bucket, bucketKey, Some(ByteRange(bytesRangeStart, bytesRangeEnd)))
    //#rangedDownload

    val Some((s3Source: Source[ByteString, _], _)) = downloadResult.runWith(Sink.head).futureValue
    val result: Future[Array[Byte]] = s3Source.map(_.toArray).runWith(Sink.head)

    result.futureValue shouldBe rangeOfBody
  }

  it should "download a stream of bytes using customer server side encryption" in {

    mockDownloadSSEC()

    val downloadResult = S3.download(bucket, bucketKey, sse = Some(sseCustomerKeys))

    val Some((s3Source: Source[ByteString, _], _)) = downloadResult.runWith(Sink.head).futureValue
    val result = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe bodySSE
  }

  it should "download a stream of bytes using customer server side encryption with version" in {
    val versionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"
    mockDownloadSSECWithVersion(versionId)

    val downloadResult =
      S3.download(bucket, bucketKey, versionId = Some(versionId), sse = Some(sseCustomerKeys))

    val Some((s3Source: Source[ByteString, _], metadata)) = downloadResult.runWith(Sink.head).futureValue
    val result = s3Source.map(_.utf8String).runWith(Sink.head)

    result.futureValue shouldBe bodySSE
    metadata.versionId.fold(fail("unable to get versionId from S3")) { vId =>
      vId shouldEqual versionId
    }
  }

  it should "fail if request returns 404" in {

    mock404s()

    val download = S3
      .download("nonexisting_bucket", "nonexisting_file.xml")
      .runWith(Sink.head)
      .futureValue

    download shouldBe None
  }

  it should "fail if download using server side encryption returns 'Invalid Request'" in {

    mockSSEInvalidRequest()

    import system.dispatcher

    val sse = ServerSideEncryption.customerKeys("encoded-key").withMd5("md5-encoded-key")
    val result = S3
      .download(bucket, bucketKey, sse = Some(sse))
      .runWith(Sink.head)
      .flatMap {
        case Some((downloadSource, _)) =>
          downloadSource
            .map(_.decodeString("utf8"))
            .runWith(Sink.head)
            .map(Some.apply)
        case None => Future.successful(None)
      }

    whenReady(result.failed) { e =>
      e shouldBe a[S3Exception]
      e.asInstanceOf[S3Exception].code should equal("InvalidRequest")
    }
  }

  it should "list keys for a given bucket with a prefix" in {
    mockListBucket()

    //#list-bucket
    val keySource: Source[ListBucketResultContents, NotUsed] =
      S3.listBucket(bucket, Some(listPrefix))
    //#list-bucket

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  it should "list keys for a given bucket with a prefix using the version 1 api" in {
    mockListBucketVersion1()

    //#list-bucket-attributes
    val useVersion1Api = S3Ext(system).settings
      .withListBucketApiVersion(ApiVersion.ListBucketVersion1)

    val keySource: Source[ListBucketResultContents, NotUsed] =
      S3.listBucket(bucket, Some(listPrefix))
        .withAttributes(S3Attributes.settings(useVersion1Api))
    //#list-bucket-attributes

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
