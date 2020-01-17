/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{`Raw-Request-URI`, ByteRange, RawHeader}
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption, StorageClass}
import akka.stream.alpakka.s3.{ApiVersion, BufferType, MemoryBufferType, MetaHeaders, S3Headers, S3Settings}
import akka.stream.scaladsl.Source
import akka.testkit.{SocketUtil, TestKit, TestProbe}
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.providers._
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.regions.Region
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpRequestsSpec extends AnyFlatSpec with Matchers with ScalaFutures {

  // test fixtures
  def getSettings(
      bufferType: BufferType = MemoryBufferType,
      awsCredentials: AwsCredentialsProvider = AnonymousCredentialsProvider.create(),
      s3Region: Region = Region.US_EAST_1,
      listBucketApiVersion: ApiVersion = ApiVersion.ListBucketVersion2
  ) = {
    val regionProvider = new AwsRegionProvider {
      def getRegion = s3Region
    }

    S3Settings(bufferType, awsCredentials, regionProvider, listBucketApiVersion)
  }

  val location = S3Location("bucket", "image-1024@2x")
  val contentType = MediaTypes.`image/jpeg`
  val acl = CannedAcl.PublicRead
  val metaHeaders: Map[String, String] = Map("location" -> "San Francisco", "orientation" -> "portrait")
  val multipartUpload = MultipartUpload(S3Location("test-bucket", "testKey"), "uploadId")

  it should "initiate multipart upload when the region is us-east-1" in {
    implicit val settings = getSettings()

    val req =
      HttpRequests.initiateMultipartUploadRequest(
        location,
        contentType,
        S3Headers().withCannedAcl(acl).withMetaHeaders(MetaHeaders(metaHeaders)).headers
      )

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/image-1024@2x"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }

  it should "initiate multipart upload with other regions" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)

    val req =
      HttpRequests.initiateMultipartUploadRequest(
        location,
        contentType,
        S3Headers().withCannedAcl(acl).withMetaHeaders(MetaHeaders(metaHeaders)).headers
      )

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3-us-east-2.amazonaws.com"
    req.uri.path.toString shouldEqual "/image-1024@2x"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }

  it should "throw an error if path-style access is false and the bucket name contains non-LDH characters" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1)

    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("invalid_bucket_name", "image-1024@2x"))
    )
  }

  it should "throw an error if the key uses `..`" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1)

    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("validbucket", "../other-bucket/image-1024@2x"))
    )
  }

  it should "throw an error when using `..` with path-style access" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withPathStyleAccess(true)

    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("invalid/../bucket_name", "image-1024@2x"))
    )
    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("../bucket_name", "image-1024@2x"))
    )
    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("bucket_name/..", "image-1024@2x"))
    )
    assertThrows[IllegalUriException](
      HttpRequests.getDownloadRequest(S3Location("..", "image-1024@2x"))
    )
  }

  it should "initiate multipart upload with path-style access in region us-east-1" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_EAST_1).withPathStyleAccess(true)

    val req =
      HttpRequests.initiateMultipartUploadRequest(
        location,
        contentType,
        S3Headers().withCannedAcl(acl).withMetaHeaders(MetaHeaders(metaHeaders)).headers
      )

    req.uri.authority.host.toString shouldEqual "s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
  }

  it should "support download requests with path-style access in region us-east-1" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_EAST_1).withPathStyleAccess(true)

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
    req.uri.rawQueryString shouldBe empty
  }

  it should "initiate multipart upload with path-style access in other regions" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_WEST_2).withPathStyleAccess(true)

    val req =
      HttpRequests.initiateMultipartUploadRequest(
        location,
        contentType,
        S3Headers().withCannedAcl(acl).withMetaHeaders(MetaHeaders(metaHeaders)).headers
      )

    req.uri.authority.host.toString shouldEqual "s3-us-west-2.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
  }

  it should "support download requests with path-style access in other regions" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withPathStyleAccess(true)

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "s3-eu-west-1.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
    req.uri.rawQueryString shouldBe empty
  }

  it should "support download requests via configured `endpointUrl`" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withEndpointUrl("http://localhost:8080")

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.scheme shouldEqual "http"
    req.uri.authority.host.address shouldEqual "localhost"
    req.uri.authority.port shouldEqual 8080
  }

  it should "support download requests with keys starting with /" in {
    // the official client supports this and this translates
    // into an object at path /[empty string]/...
    // added this test because of a tricky uri building issue
    // in case of pathStyleAccess = false
    implicit val settings = getSettings()

    val location = S3Location("bucket", "/test/foo.txt")

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "//test/foo.txt"
    req.uri.rawQueryString shouldBe empty
  }

  it should "support download requests with keys ending with /" in {
    // object with a slash at the end of the filename should be accessible
    implicit val settings = getSettings()

    val location = S3Location("bucket", "/test//")

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "//test//"
    req.uri.rawQueryString shouldBe empty
  }

  it should "support download requests with keys containing spaces" in {
    implicit val settings = getSettings()

    val location = S3Location("bucket", "test folder/test file.txt")

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/test%20folder/test%20file.txt"
    req.uri.rawQueryString shouldBe empty
  }

  it should "support download requests with keys containing plus" in {
    implicit val settings = getSettings()

    val location = S3Location("bucket", "test folder/1 + 2 = 3")
    val req = HttpRequests.getDownloadRequest(location)
    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/test%20folder/1%20+%202%20=%203"
    req.headers should contain(`Raw-Request-URI`("/test%20folder/1%20%2B%202%20=%203"))
  }

  it should "support download requests with keys containing spaces with path-style access in other regions" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withPathStyleAccess(true)

    val location = S3Location("bucket", "test folder/test file.txt")

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "s3-eu-west-1.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/test%20folder/test%20file.txt"
    req.uri.rawQueryString shouldBe empty
  }

  it should "add versionId query parameter when provided" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings().withPathStyleAccess(true)

    val location = S3Location("bucket", "test/foo.txt")
    val versionId = "123456"
    val req = HttpRequests.getDownloadRequest(location, versionId = Some(versionId))

    req.uri.authority.host.toString shouldEqual "s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/test/foo.txt"
    req.uri.rawQueryString.fold(fail("query string is empty while it was supposed to be populated")) { rawQueryString =>
      rawQueryString shouldEqual s"versionId=$versionId"
    }
  }

  it should "support multipart init upload requests via configured `endpointUrl`" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withEndpointUrl("http://localhost:8080")

    val req =
      HttpRequests.initiateMultipartUploadRequest(
        location,
        contentType,
        S3Headers().withCannedAcl(acl).withMetaHeaders(MetaHeaders(metaHeaders)).headers
      )

    req.uri.scheme shouldEqual "http"
    req.uri.authority.host.address shouldEqual "localhost"
    req.uri.authority.port shouldEqual 8080
  }

  it should "support multipart upload part requests via configured `endpointUrl`" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withEndpointUrl("http://localhost:8080")

    val req =
      HttpRequests.uploadPartRequest(multipartUpload, 1, Source.empty, 1)

    req.uri.scheme shouldEqual "http"
    req.uri.authority.host.address shouldEqual "localhost"
    req.uri.authority.port shouldEqual 8080
  }

  it should "properly multipart upload part request with customer keys server side encryption" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withPathStyleAccess(true)
    val myKey = "my-key"
    val md5Key = "md5-key"
    val s3Headers = ServerSideEncryption.customerKeys(myKey).withMd5(md5Key).headersFor(UploadPart)
    val req = HttpRequests.uploadPartRequest(multipartUpload, 1, Source.empty, 1, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", myKey))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))
  }

  it should "support multipart upload complete requests via configured `endpointUrl`" in {
    implicit val settings = getSettings(s3Region = Region.EU_WEST_1).withEndpointUrl("http://localhost:8080")
    implicit val executionContext = scala.concurrent.ExecutionContext.global

    val req =
      HttpRequests.completeMultipartUploadRequest(multipartUpload, (1, "part") :: Nil, Nil).futureValue

    req.uri.scheme shouldEqual "http"
    req.uri.authority.host.address shouldEqual "localhost"
    req.uri.authority.port shouldEqual 8080
  }

  it should "initiate multipart upload with AES-256 server side encryption" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)
    val s3Headers = ServerSideEncryption.aes256().headersFor(InitiateMultipartUpload)
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))
  }

  it should "initiate multipart upload with aws:kms server side encryption" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)
    val testArn = "arn:aws:kms:my-region:my-account-id:key/my-key-id"
    val s3Headers = ServerSideEncryption.kms(testArn).headersFor(InitiateMultipartUpload)
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-aws-kms-key-id", testArn))
  }

  it should "initiate multipart upload with customer keys encryption" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)
    val myKey = "my-key"
    val md5Key = "md5-key"
    val s3Headers = ServerSideEncryption.customerKeys(myKey).withMd5(md5Key).headersFor(InitiateMultipartUpload)
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", myKey))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))
  }

  it should "initiate multipart upload with custom s3 storage class" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)
    val s3Headers = S3Headers().withStorageClass(StorageClass.ReducedRedundancy).headers
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-storage-class", "REDUCED_REDUNDANCY"))
  }

  it should "initiate multipart upload with custom s3 headers" in {
    implicit val settings = getSettings(s3Region = Region.US_EAST_2)
    val s3Headers = S3Headers().withCustomHeaders(Map("Cache-Control" -> "no-cache")).headers
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("Cache-Control", "no-cache"))
  }

  it should "properly construct the list bucket request with no prefix, continuation token or delimiter passed" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_EAST_2).withPathStyleAccess(true)

    val req =
      HttpRequests.listBucket(location.bucket)

    req.uri.query() shouldEqual Query("list-type" -> "2")
  }

  it should "properly construct the list bucket request with a prefix and token passed" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_EAST_2).withPathStyleAccess(true)

    val req =
      HttpRequests.listBucket(location.bucket, Some("random/prefix"), Some("randomToken"))

    req.uri.query() shouldEqual Query("list-type" -> "2",
                                      "prefix" -> "random/prefix",
                                      "continuation-token" -> "randomToken")
  }

  it should "properly construct the list bucket request with a delimiter and token passed" in {
    @com.github.ghik.silencer.silent
    implicit val settings = getSettings(s3Region = Region.US_EAST_2).withPathStyleAccess(true)

    val req =
      HttpRequests.listBucket(location.bucket, delimiter = Some("/"), continuationToken = Some("randomToken"))

    req.uri.query() shouldEqual Query("list-type" -> "2", "delimiter" -> "/", "continuation-token" -> "randomToken")
  }

  it should "properly construct the list bucket request when using api version 1" in {
    @com.github.ghik.silencer.silent
    implicit val settings =
      getSettings(s3Region = Region.US_EAST_2, listBucketApiVersion = ApiVersion.ListBucketVersion1)
        .withPathStyleAccess(true)

    val req =
      HttpRequests.listBucket(location.bucket)

    req.uri.query() shouldEqual Query()
  }

  it should "properly construct the list bucket request when using api version set to 1 and a continuation token" in {
    @com.github.ghik.silencer.silent
    implicit val settings =
      getSettings(s3Region = Region.US_EAST_2, listBucketApiVersion = ApiVersion.ListBucketVersion1)
        .withPathStyleAccess(true)

    val req =
      HttpRequests.listBucket(location.bucket, continuationToken = Some("randomToken"))

    req.uri.query() shouldEqual Query("marker" -> "randomToken")
  }

  it should "support custom endpoint configured by `endpointUrl`" in {
    implicit val system: ActorSystem = ActorSystem("HttpRequestsSpec")
    import system.dispatcher
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val probe = TestProbe()
    val address = SocketUtil.temporaryServerAddress()

    import akka.http.scaladsl.server.Directives._

    Http().bindAndHandle(extractRequestContext { ctx =>
      probe.ref ! ctx.request
      complete("MOCK")
    }, address.getHostName, address.getPort)

    implicit val setting: S3Settings =
      getSettings().withEndpointUrl(s"http://${address.getHostName}:${address.getPort}/")

    val req =
      HttpRequests.listBucket(location.bucket, Some("random/prefix"), Some("randomToken"))

    Http().singleRequest(req).futureValue

    probe.expectMsgType[HttpRequest]

    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  it should "add two (source, range) headers to multipart upload (copy) request when byte range populated" in {
    implicit val settings: S3Settings = getSettings()

    val multipartUpload = MultipartUpload(S3Location("target-bucket", "target-key"), UUID.randomUUID().toString)
    val copyPartition = CopyPartition(1, S3Location("source-bucket", "some/source-key"), Some(ByteRange(0, 5242880L)))
    val multipartCopy = MultipartCopy(multipartUpload, copyPartition)

    val request = HttpRequests.uploadCopyPartRequest(multipartCopy)
    request.headers should contain(RawHeader("x-amz-copy-source", "/source-bucket/some/source-key"))
    request.headers should contain(RawHeader("x-amz-copy-source-range", "bytes=0-5242879"))
  }

  it should "add only source header to multipart upload (copy) request when byte range missing" in {
    implicit val settings: S3Settings = getSettings()

    val multipartUpload = MultipartUpload(S3Location("target-bucket", "target-key"), UUID.randomUUID().toString)
    val copyPartition = CopyPartition(1, S3Location("source-bucket", "some/source-key"))
    val multipartCopy = MultipartCopy(multipartUpload, copyPartition)

    val request = HttpRequests.uploadCopyPartRequest(multipartCopy)
    request.headers should contain(RawHeader("x-amz-copy-source", "/source-bucket/some/source-key"))
    request.headers.map(_.lowercaseName()) should not contain "x-amz-copy-source-range"
  }

  it should "add versionId parameter to source header if provided" in {
    implicit val settings: S3Settings = getSettings()

    val multipartUpload = MultipartUpload(S3Location("target-bucket", "target-key"), UUID.randomUUID().toString)
    val copyPartition = CopyPartition(1, S3Location("source-bucket", "some/source-key"), Some(ByteRange(0, 5242880L)))
    val multipartCopy = MultipartCopy(multipartUpload, copyPartition)

    val request = HttpRequests.uploadCopyPartRequest(multipartCopy, Some("abcdwxyz"))
    request.headers should contain(RawHeader("x-amz-copy-source", "/source-bucket/some/source-key?versionId=abcdwxyz"))
    request.headers should contain(RawHeader("x-amz-copy-source-range", "bytes=0-5242879"))
  }

  it should "create make bucket request" in {
    implicit val settings: S3Settings = getSettings()

    val request = HttpRequests.bucketManagementRequest(location, method = HttpMethods.PUT)

    //Date is added by akka by default
    request.uri.authority.host.toString should equal("bucket.s3.amazonaws.com")
    request.entity.contentLengthOption should equal(Some(0))
    request.uri.queryString() should equal(None)
    request.method should equal(HttpMethods.PUT)
  }

  it should "create delete bucket request" in {
    implicit val settings: S3Settings = getSettings()

    val request = HttpRequests.bucketManagementRequest(location, method = HttpMethods.DELETE)

    //Date is added by akka by default
    request.uri.authority.host.toString should equal("bucket.s3.amazonaws.com")
    request.entity.contentLengthOption should equal(Some(0))
    request.uri.queryString() should equal(None)
    request.method should equal(HttpMethods.DELETE)
  }

  it should "create checkIfExits bucket request" in {
    implicit val settings: S3Settings = getSettings()

    val request: HttpRequest = HttpRequests.bucketManagementRequest(location, method = HttpMethods.HEAD)

    //Date is added by akka by default
    request.uri.authority.host.toString should equal("bucket.s3.amazonaws.com")
    request.entity.contentLengthOption should equal(Some(0))
    request.uri.queryString() should equal(None)
    request.method should equal(HttpMethods.HEAD)
  }
}
