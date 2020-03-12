/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.ClientTransport
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.settings.ClientConnectionSettings
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.s3.BucketAccess.{AccessGranted, NotExists}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.testkit.TestKit
import akka.util.ByteString
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.providers._
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import software.amazon.awssdk.regions.Region

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait S3IntegrationSpec
    extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with OptionValues
    with LogCapturing {

  implicit val actorSystem: ActorSystem = ActorSystem(
    "S3IntegrationSpec",
    config().withFallback(ConfigFactory.load())
  )
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  implicit val defaultPatience: PatienceConfig = PatienceConfig(90.seconds, 30.millis)

  val defaultBucket = "my-test-us-east-1"
  val nonExistingBucket = "nowhere"

  // with dots forcing path style access
  val bucketWithDots = "my.test.frankfurt"

  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(actorSystem)

  def config() = ConfigFactory.parseString("""
      |alpakka.s3.aws.region {
      |  provider = static
      |  default-region = "us-east-1"
      |}
    """.stripMargin)

  /** Hooks for Minio tests to overwrite the HTTP transport */
  def attributes: Attributes = Attributes.none

  /** Hooks for Minio tests to overwrite the HTTP transport */
  def attributes(s3Settings: S3Settings): Attributes = Attributes.none

  @com.github.ghik.silencer.silent("path-style access")
  def otherRegionSettingsPathStyleAccess =
    S3Settings()
      .withPathStyleAccess(true)
      .withS3RegionProvider(new AwsRegionProvider {
        val getRegion: Region = Region.EU_CENTRAL_1
      })

  /** Empty settings to be override in MinioSpec  */
  def invalidCredentials = S3Settings()

  def defaultRegionContentCount = 4
  def otherRegionContentCount = 5

  it should "list with real credentials" in {
    val result = S3
      .listBucket(defaultBucket, None)
      .withAttributes(attributes)
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials using the Version 1 API" in {
    val result = S3
      .listBucket(defaultBucket, None)
      .withAttributes(attributes(S3Settings().withListBucketApiVersion(ApiVersion.ListBucketVersion1)))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials in non us-east-1 zone" in {
    val result = S3
      .listBucket(bucketWithDots, None)
      .withAttributes(attributes(otherRegionSettingsPathStyleAccess))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe otherRegionContentCount
  }

  it should "upload with real credentials" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result =
      S3.putObject(defaultBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .withAttributes(attributes)
        .runWith(Sink.head)

    val uploadResult = Await.ready(result, 90.seconds).futureValue
    uploadResult.eTag should not be empty
  }

  it should "upload and delete" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result = for {
      put <- S3
        .putObject(defaultBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .withAttributes(attributes)
        .runWith(Sink.head)
      metaBefore <- S3.getObjectMetadata(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      delete <- S3.deleteObject(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      metaAfter <- S3.getObjectMetadata(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
    } yield {
      (put, delete, metaBefore, metaAfter)
    }

    val (putResult, deleteResult, metaBefore, metaAfter) = Await.ready(result, 90.seconds).futureValue
    putResult.eTag should not be empty
    metaBefore should not be empty
    metaBefore.get.contentType shouldBe Some(ContentTypes.`application/octet-stream`.value)
    metaAfter shouldBe empty
  }

  it should "upload multipart with real credentials" in {
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val result =
      source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )

    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" in {
    val Some((source, meta)) =
      Await
        .ready(S3.download(defaultBucket, objectKey)
                 .withAttributes(attributes)
                 .runWith(Sink.head),
               5.seconds)
        .futureValue

    val bodyFuture = source
      .map(_.decodeString("utf8"))
      .toMat(Sink.head)(Keep.right)
      .run()

    val body = Await.ready(bodyFuture, 5.seconds).futureValue
    body shouldBe objectValue
    meta.eTag should not be empty
    meta.contentType shouldBe Some(ContentTypes.`application/octet-stream`.value)
  }

  it should "delete with real credentials" in {
    val delete = S3
      .deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
    delete.futureValue shouldEqual akka.Done
  }

  it should "upload huge multipart with real credentials" in {
    val objectKey = "huge"
    val hugeString = "0123456789abcdef" * 64 * 1024 * 11
    val result =
      Source
        .single(ByteString(hugeString))
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )

    val multipartUploadResult = result.futureValue
    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "upload, download and delete with spaces in the key" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )
      download <- S3.download(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head).flatMap {
        case Some((downloadSource, _)) =>
          downloadSource
            .map(_.decodeString("utf8"))
            .runWith(Sink.head)
        case None => Future.successful(None)
      }
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with brackets in the key" in {
    val objectKey = "abc/DEF/2017/06/15/1234 (1).TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )
      download <- S3
        .download(defaultBucket, objectKey)
        .withAttributes(attributes)
        .runWith(Sink.head)
        .flatMap {
          case Some((downloadSource, _)) =>
            downloadSource
              .map(_.decodeString("utf8"))
              .runWith(Sink.head)
          case None => Future.successful(None)
        }
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with spaces in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "test folder/test file.txt"
  )

  // we want ASCII and other UTF-8 characters!
  it should "upload, download and delete with special characters in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "føldęrü/1234()[]><!? .TXT"
  )

  it should "upload, download and delete with `+` character in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "1 + 2 = 3"
  )

  it should "upload, copy, download the copy, and delete" in {
    val sourceKey = "original/file.txt"
    val targetKey = "copy/file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey).withAttributes(attributes))
      copy <- S3
        .multipartCopy(defaultBucket, sourceKey, defaultBucket, targetKey)
        .withAttributes(attributes)
        .run()
      download <- S3
        .download(defaultBucket, targetKey)
        .withAttributes(attributes)
        .runWith(Sink.head)
        .flatMap {
          case Some((downloadSource, _)) =>
            downloadSource
              .map(_.decodeString("utf8"))
              .runWith(Sink.head)
          case None => Future.successful(None)
        }
    } yield (upload, copy, download)

    whenReady(results) {
      case (upload, copy, downloaded) =>
        upload.bucket shouldEqual defaultBucket
        upload.key shouldEqual sourceKey
        copy.bucket shouldEqual defaultBucket
        copy.key shouldEqual targetKey
        downloaded shouldBe objectValue

        S3.deleteObject(defaultBucket, sourceKey)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
        S3.deleteObject(defaultBucket, targetKey)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
    }
  }

  it should "upload 2 files with common prefix, 1 with different prefix and delete by prefix" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val sourceKey3 = "uploaded/file3.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey1).withAttributes(attributes))
      upload2 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey2).withAttributes(attributes))
      upload3 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey3).withAttributes(attributes))
    } yield (upload1, upload2, upload3)

    whenReady(results) {
      case (upload1, upload2, upload3) =>
        upload1.bucket shouldEqual defaultBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultBucket
        upload2.key shouldEqual sourceKey2
        upload3.bucket shouldEqual defaultBucket
        upload3.key shouldEqual sourceKey3

        S3.deleteObjectsByPrefix(defaultBucket, Some("original"))
          .withAttributes(attributes)
          .runWith(Sink.ignore)
          .futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultBucket, Some("original"))
            .withAttributes(attributes)
            .runFold(0)((result, _) => result + 1)
            .futureValue
        numOfKeysForPrefix shouldEqual 0
        S3.deleteObject(defaultBucket, sourceKey3)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
    }
  }

  it should "upload 2 files, delete all files in bucket" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey1).withAttributes(attributes))
      upload2 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey2).withAttributes(attributes))
    } yield (upload1, upload2)

    whenReady(results) {
      case (upload1, upload2) =>
        upload1.bucket shouldEqual defaultBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultBucket
        upload2.key shouldEqual sourceKey2

        S3.deleteObjectsByPrefix(defaultBucket, prefix = None)
          .withAttributes(attributes)
          .runWith(Sink.ignore)
          .futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultBucket, None)
            .withAttributes(attributes)
            .runFold(0)((result, _) => result + 1)
            .futureValue
        numOfKeysForPrefix shouldEqual 0
    }
  }

  it should "make a bucket with given name" in {
    implicit val attr: Attributes = attributes
    val bucketName = "samplebucket1"

    val request: Future[Done] = S3
      .makeBucket(bucketName)

    whenReady(request) { value =>
      value shouldEqual Done

      Await.result(S3.deleteBucket(bucketName), Duration(1, TimeUnit.MINUTES))
    }
  }

  it should "throw an exception while creating a bucket with the same name" in {
    implicit val attr: Attributes = attributes
    assertThrows[S3Exception] {
      val result: Future[Done] = S3.makeBucket(defaultBucket)

      Await.result(result, Duration(1, TimeUnit.MINUTES))
    }
  }

  it should "create and delete bucket with a given name" in {
    val bucketName = "samplebucket3"

    val makeRequest: Source[Done, NotUsed] = S3
      .makeBucketSource(bucketName)
      .withAttributes(attributes)
    val deleteRequest: Source[Done, NotUsed] = S3
      .deleteBucketSource(bucketName)
      .withAttributes(attributes)

    val request = for {
      make <- makeRequest.runWith(Sink.ignore)
      delete <- deleteRequest.runWith(Sink.ignore)
    } yield (make, delete)

    val response = Await.result(request, Duration(1, TimeUnit.MINUTES))
    response should equal((Done, Done))
  }

  it should "throw an exception while deleting bucket that doesn't exist" in {
    implicit val attr: Attributes = attributes
    assertThrows[S3Exception] {
      val result: Future[Done] = S3.deleteBucket(nonExistingBucket)

      Await.result(result, Duration(1, TimeUnit.MINUTES))
    }
  }

  it should "check if bucket exists" in {
    implicit val attr: Attributes = attributes
    val checkIfBucketExits: Future[BucketAccess] = S3.checkIfBucketExists(defaultBucket)

    whenReady(checkIfBucketExits) { bucketState =>
      bucketState should equal(AccessGranted)
    }
  }

  it should "check for non-existing bucket" in {
    implicit val attr: Attributes = attributes
    val request: Future[BucketAccess] = S3.checkIfBucketExists(nonExistingBucket)

    whenReady(request) { response =>
      response should equal(NotExists)
    }
  }

  it should "contain error code even if exception in empty" in {
    val exception = intercept[S3Exception] {
      val result = S3
        .getObjectMetadata(defaultBucket, "sample")
        .withAttributes(attributes(invalidCredentials))
        .runWith(Sink.head)

      Await.result(result, Duration(1, TimeUnit.MINUTES))
    }

    exception.code shouldBe StatusCodes.Forbidden.toString()
  }

  private def uploadDownloadAndDeleteInOtherRegionCase(objectKey: String): Assertion = {
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(bucketWithDots, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(S3Attributes.settings(otherRegionSettingsPathStyleAccess))
        )
      download <- S3
        .download(bucketWithDots, objectKey)
        .withAttributes(S3Attributes.settings(otherRegionSettingsPathStyleAccess))
        .runWith(Sink.head)
        .flatMap {
          case Some((downloadSource, _)) =>
            downloadSource
              .map(_.decodeString("utf8"))
              .runWith(Sink.head)
          case None => Future.successful(None)
        }
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe bucketWithDots
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(bucketWithDots, objectKey)
      .withAttributes(S3Attributes.settings(otherRegionSettingsPathStyleAccess))
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }
}

/*
 * This is an integration test and ignored by default
 *
 * For running the tests you need to create 2 buckets:
 *  - one in region us-east-1
 *  - one in an other region (eg eu-central-1)
 * Update the bucket name and regions in the code below
 *
 * Set your keys aws access-key-id and secret-access-key in src/test/resources/application.conf
 *
 * Comment @ignore and run the tests
 * (tests that do listing counts might need some tweaking)
 *
 */
@Ignore
class AWSS3IntegrationSpec extends S3IntegrationSpec

/*
 * For this test, you need a local s3 mirror, for instance minio (https://github.com/minio/minio).
 * With docker and the aws cli installed, you could run something like this:
 *
 * docker run -e MINIO_ACCESS_KEY=TESTKEY -e MINIO_SECRET_KEY=TESTSECRET -p 9000:9000 minio/minio server /data
 * AWS_ACCESS_KEY_ID=TESTKEY AWS_SECRET_ACCESS_KEY=TESTSECRET aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket my-test-us-east-1
 * AWS_ACCESS_KEY_ID=TESTKEY AWS_SECRET_ACCESS_KEY=TESTSECRET aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket my.test.frankfurt
 *
 * Run the tests from inside sbt:
 * s3/testOnly *.MinioS3IntegrationSpec
 */
class MinioS3IntegrationSpec extends S3IntegrationSpec {
  import MinioS3IntegrationSpec._

  override val defaultRegionContentCount = 0
  override val otherRegionContentCount = 0

  override def config() =
    ConfigFactory.parseString(s"""
                                 |alpakka.s3 {
                                 |  aws {
                                 |    credentials {
                                 |      provider = static
                                 |      access-key-id = $accessKey
                                 |      secret-access-key = $secret
                                 |    }
                                 |  }
                                 |  endpoint-url = "$endpointUrlDns"
                                 |}
    """.stripMargin).withFallback(super.config())

  @com.github.ghik.silencer.silent
  override def otherRegionSettingsPathStyleAccess =
    S3Settings()
      .withCredentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secret)))
      .withEndpointUrl(endpointUrlPathStyle)
      .withPathStyleAccess(true)

  override def invalidCredentials: S3Settings =
    S3Settings()
      .withCredentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("invalid", "invalid"))
      )

  override def attributes: Attributes = attributes(S3Settings()(actorSystem))

  override def attributes(s3Settings: S3Settings): Attributes = {
    S3Attributes.settings(
      s3Settings
        .withForwardProxy(
          ForwardProxy.transport(ShimTransport(InetSocketAddress.createUnresolved("localhost", 9000)))
        )
    )
  }

  private case class ShimTransport(address: InetSocketAddress) extends ClientTransport {
    def connectTo(host: String, port: Int, settings: ClientConnectionSettings)(
        implicit system: ActorSystem
    ): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
      Tcp()(system)
        .outgoingConnection(
          address,
          settings.localAddress,
          settings.socketOptions,
          halfClose = true,
          settings.connectingTimeout,
          settings.idleTimeout
        )
        .mapMaterializedValue(
          _.map(tcpConn => OutgoingConnection(tcpConn.localAddress, tcpConn.remoteAddress))(system.dispatcher)
        )
  }

  it should "properly set the endpointUrl" in {
    S3Settings().endpointUrl.value shouldEqual endpointUrlDns
  }
}

object MinioS3IntegrationSpec {
  val accessKey = "TESTKEY"
  val secret = "TESTSECRET"
  val endpointUrlPathStyle = "http://localhost:9000"
  val localMinioDomain = "s3minio.alpakka"
  val endpointUrlDns = s"http://{bucket}.$localMinioDomain:9000"
}
