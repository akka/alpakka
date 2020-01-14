/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpResponse, IllegalUriException}
import akka.stream.Attributes
import akka.stream.alpakka.s3.BucketAccess.{AccessDenied, AccessGranted, NotExists}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.headers.ServerSideEncryption
import akka.stream.alpakka.s3.scaladsl.{S3, S3ClientIntegrationSpec, S3WireMockBase}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers._

import scala.concurrent.Future

class S3SourceSpec extends S3WireMockBase with S3ClientIntegrationSpec {
  private val sampleSettings = S3Ext(system).settings

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

    val region = Region.AP_NORTHEAST_1

    mockDownload(region)

    val customRegion = S3Ext(system).settings
      .withS3RegionProvider(new AwsRegionProvider {
        override def getRegion: Region = region
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
      .download("nonexisting-bucket", "nonexisting_file.xml")
      .runWith(Sink.head)
      .futureValue

    download shouldBe None
  }

  it should "fail for illegal bucket names" in {
    val dnsStyleAccess = S3Ext(system).settings
      .withPathStyleAccess(true)
      .withEndpointUrl(null)

    val download = S3
      .download("path/../with-dots", "unused")
      .withAttributes(S3Attributes.settings(dnsStyleAccess))
      .runWith(Sink.head)

    download.failed.futureValue shouldBe an[IllegalUriException]
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

  it should "list keys and common prefixes for a given bucket with a prefix and delimiter" in {
    mockListBucketAndCommonPrefixes()

    //#list-bucket-and-common-prefixes
    val keyAndCommonPrefixSource
        : Source[(Seq[ListBucketResultContents], Seq[ListBucketResultCommonPrefixes]), NotUsed] =
      S3.listBucketAndCommonPrefixes(bucket, listDelimiter, Some(listPrefix))
    //#list-bucket-and-common-prefixes

    val result = keyAndCommonPrefixSource.runWith(Sink.head)
    val contents = result.futureValue._1
    val commonPrefixes = result.futureValue._2

    contents.head.key shouldBe listKey
    commonPrefixes.head.prefix shouldBe listCommonPrefix
  }

  it should "list keys and common prefixes for a given bucket with a prefix and delimiter using the version 1 api" in {
    mockListBucketAndCommonPrefixesVersion1()

    val useVersion1Api = S3Ext(system).settings
      .withListBucketApiVersion(ApiVersion.ListBucketVersion1)

    val keyAndCommonPrefixSource
        : Source[(Seq[ListBucketResultContents], Seq[ListBucketResultCommonPrefixes]), NotUsed] =
      S3.listBucketAndCommonPrefixes(bucket, listDelimiter, Some(listPrefix))
        .withAttributes(S3Attributes.settings(useVersion1Api))

    val result = keyAndCommonPrefixSource.runWith(Sink.head)
    val contents = result.futureValue._1
    val commonPrefixes = result.futureValue._2

    contents.head.key shouldBe listKey
    commonPrefixes.head.prefix shouldBe listCommonPrefix
  }

  it should "list keys for a given bucket with a delimiter and prefix" in {
    mockListBucketAndCommonPrefixes()

    //#list-bucket-delimiter
    val keySource: Source[ListBucketResultContents, NotUsed] =
      S3.listBucket(bucket, listDelimiter, Some(listPrefix))
    //#list-bucket-delimiter

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  it should "list keys for a given bucket with a delimiter and prefix using the version 1 api" in {
    mockListBucketAndCommonPrefixesVersion1()

    val useVersion1Api = S3Ext(system).settings
      .withListBucketApiVersion(ApiVersion.ListBucketVersion1)

    val keySource: Source[ListBucketResultContents, NotUsed] =
      S3.listBucket(bucket, listDelimiter, Some(listPrefix))
        .withAttributes(S3Attributes.settings(useVersion1Api))

    val result = keySource.runWith(Sink.head)

    result.futureValue.key shouldBe listKey
  }

  it should "make a bucket with given name" in {
    mockMakingBucket()

    //#make-bucket
    val bucketName = "samplebucket1"

    implicit val sampleAttributes: Attributes = S3Attributes.settings(sampleSettings)

    val makeBucketRequest: Future[Done] = S3.makeBucket(bucketName)
    val makeBucketSourceRequest: Source[Done, NotUsed] = S3.makeBucketSource(bucketName)
    //#make-bucket

    makeBucketRequest.futureValue shouldBe Done
    makeBucketSourceRequest.runWith(Sink.ignore).futureValue shouldBe Done
  }

  it should "delete a bucket with given name" in {
    val bucketName = "samplebucket1"

    mockDeletingBucket()

    //#delete-bucket
    implicit val sampleAttributes: Attributes = S3Attributes.settings(sampleSettings)

    val deleteBucketRequest: Future[Done] = S3.deleteBucket(bucketName)
    val deleteBucketSourceRequest: Source[Done, NotUsed] = S3.deleteBucketSource(bucketName)
    //#delete-bucket

    deleteBucketRequest.futureValue shouldBe Done
    deleteBucketSourceRequest.runWith(Sink.ignore).futureValue shouldBe Done
  }

  it should "check for non existing buckets" in {
    mockCheckingBucketStateForNonExistingBucket()

    //#check-if-bucket-exists
    implicit val sampleAttributes: Attributes = S3Attributes.settings(sampleSettings)

    val doesntExistRequest: Future[BucketAccess] = S3.checkIfBucketExists(bucket)
    val doesntExistSourceRequest: Source[BucketAccess, NotUsed] = S3.checkIfBucketExistsSource(bucket)
    //#check-if-bucket-exists

    doesntExistRequest.futureValue shouldBe NotExists

    doesntExistSourceRequest.runWith(Sink.head).futureValue shouldBe NotExists
  }

  it should "check for existing buckets" in {
    mockCheckingBucketStateForExistingBucket()

    val existRequest: Future[BucketAccess] = S3.checkIfBucketExists(bucket)
    val existSourceRequest: Source[BucketAccess, NotUsed] = S3.checkIfBucketExistsSource(bucket)

    existRequest.futureValue shouldBe AccessGranted

    existSourceRequest.runWith(Sink.head).futureValue shouldBe AccessGranted
  }

  it should "check for buckets without rights" in {
    mockCheckingBucketStateForBucketWithoutRights()

    val noRightsRequest: Future[BucketAccess] = S3.checkIfBucketExists(bucket)
    val noRightsSourceRequest: Source[BucketAccess, NotUsed] = S3.checkIfBucketExistsSource(bucket)

    noRightsRequest.futureValue shouldBe AccessDenied

    noRightsSourceRequest.runWith(Sink.head).futureValue shouldBe AccessDenied
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
    stopWireMockServer()
  }
}
