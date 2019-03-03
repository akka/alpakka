/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import com.amazonaws.regions.AwsRegionProvider

trait S3IntegrationSpec extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures with OptionValues {

  implicit val actorSystem: ActorSystem = ActorSystem(
    "S3IntegrationSpec",
    config().withFallback(ConfigFactory.load())
  )
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  val defaultRegionBucket = "my-test-us-east-1"

  val otherRegion = "eu-central-1"
  val otherRegionProvider = new AwsRegionProvider {
    val getRegion: String = otherRegion
  }
  val otherRegionBucket = "my.test.frankfurt" // with dots forcing path style access

  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  def config() = ConfigFactory.parseString("""
      |alpakka.s3.aws.region {
      |  provider = static
      |  default-region = "us-east-1"
      |}
    """.stripMargin)

  def otherRegionSettings =
    S3Settings().withPathStyleAccess(true).withS3RegionProvider(otherRegionProvider)
  def listBucketVersion1Settings =
    S3Settings().withListBucketApiVersion(ApiVersion.ListBucketVersion1)

  def defaultRegionContentCount = 4
  def otherRegionContentCount = 5

  it should "list with real credentials" in {
    val result = S3
      .listBucket(defaultRegionBucket, None)
      //.addAttributes(S3Attributes.settings(settings))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials using the Version 1 API" in {
    val result = S3
      .listBucket(defaultRegionBucket, None)
      .withAttributes(S3Attributes.settings(listBucketVersion1Settings))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials in non us-east-1 zone" in {
    val result = S3
      .listBucket(otherRegionBucket, None)
      .withAttributes(S3Attributes.settings(otherRegionSettings))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe otherRegionContentCount
  }

  it should "upload with real credentials" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result =
      S3.putObject(defaultRegionBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
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
        .putObject(defaultRegionBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .runWith(Sink.head)
      metaBefore <- S3.getObjectMetadata(defaultRegionBucket, objectKey).runWith(Sink.head)
      delete <- S3.deleteObject(defaultRegionBucket, objectKey).runWith(Sink.head)
      metaAfter <- S3.getObjectMetadata(defaultRegionBucket, objectKey).runWith(Sink.head)
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
    //val source: Source[ByteString, Any] = FileIO.fromPath(Paths.get("/tmp/IMG_0470.JPG"))

    val result =
      source
        .runWith(
          S3.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
        )
        .runWith(Sink.head)

    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" in {
    val Some((source, meta)) =
      Await.ready(S3.download(defaultRegionBucket, objectKey).runWith(Sink.head), 5.seconds).futureValue

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
    val delete = S3.deleteObject(defaultRegionBucket, objectKey).runWith(Sink.head)
    delete.futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with spaces in the key" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
        )
        .runWith(Sink.head)
      download <- S3.download(defaultRegionBucket, objectKey).runWith(Sink.head).flatMap {
        case Some((downloadSource, _)) =>
          downloadSource
            .map(_.decodeString("utf8"))
            .runWith(Sink.head)
        case None => Future.successful(None)
      }
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultRegionBucket, objectKey).runWith(Sink.head).futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with brackets in the key" in {
    val objectKey = "abc/DEF/2017/06/15/1234 (1).TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
        )
        .runWith(Sink.head)
      download <- S3.download(defaultRegionBucket, objectKey).runWith(Sink.head).flatMap {
        case Some((downloadSource, _)) =>
          downloadSource
            .map(_.decodeString("utf8"))
            .runWith(Sink.head)
        case None => Future.successful(None)
      }
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultRegionBucket, objectKey).runWith(Sink.head).futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with spaces in the key in non us-east-1 zone" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(otherRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(S3Attributes.settings(otherRegionSettings))
        )
        .runWith(Sink.head)
      download <- S3
        .download(otherRegionBucket, objectKey)
        .withAttributes(S3Attributes.settings(otherRegionSettings))
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

    multipartUploadResult.bucket shouldBe otherRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(otherRegionBucket, objectKey)
      .withAttributes(S3Attributes.settings(otherRegionSettings))
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with special characters in the key in non us-east-1 zone" in {
    // we want ASCII and other UTF-8 characters!
    val objectKey = "føldęrü/1234()[]><!? .TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(otherRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(S3Attributes.settings(otherRegionSettings))
        )
        .runWith(Sink.head)
      download <- S3
        .download(otherRegionBucket, objectKey)
        .withAttributes(S3Attributes.settings(otherRegionSettings))
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

    multipartUploadResult.bucket shouldBe otherRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(otherRegionBucket, objectKey)
      .withAttributes(S3Attributes.settings(otherRegionSettings))
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, copy, download the copy, and delete" in {
    val sourceKey = "original/file.txt"
    val targetKey = "copy/file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey)).runWith(Sink.head)
      copy <- S3.multipartCopy(defaultRegionBucket, sourceKey, defaultRegionBucket, targetKey).run().runWith(Sink.head)
      download <- S3.download(defaultRegionBucket, targetKey).runWith(Sink.head).flatMap {
        case Some((downloadSource, _)) =>
          downloadSource
            .map(_.decodeString("utf8"))
            .runWith(Sink.head)
        case None => Future.successful(None)
      }
    } yield (upload, copy, download)

    whenReady(results) {
      case (upload, copy, downloaded) =>
        upload.bucket shouldEqual defaultRegionBucket
        upload.key shouldEqual sourceKey
        copy.bucket shouldEqual defaultRegionBucket
        copy.key shouldEqual targetKey
        downloaded shouldBe objectValue

        S3.deleteObject(defaultRegionBucket, sourceKey).runWith(Sink.head).futureValue shouldEqual akka.Done
        S3.deleteObject(defaultRegionBucket, targetKey).runWith(Sink.head).futureValue shouldEqual akka.Done
    }
  }

  it should "upload 2 files with common prefix, 1 with different prefix and delete by prefix" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val sourceKey3 = "uploaded/file3.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey1)).runWith(Sink.head)
      upload2 <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey2)).runWith(Sink.head)
      upload3 <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey3)).runWith(Sink.head)
    } yield (upload1, upload2, upload3)

    whenReady(results) {
      case (upload1, upload2, upload3) =>
        upload1.bucket shouldEqual defaultRegionBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultRegionBucket
        upload2.key shouldEqual sourceKey2
        upload3.bucket shouldEqual defaultRegionBucket
        upload3.key shouldEqual sourceKey3

        S3.deleteObjectsByPrefix(defaultRegionBucket, Some("original")).runWith(Sink.head).futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultRegionBucket, Some("original")).runFold(0)((result, _) => result + 1)
        numOfKeysForPrefix shouldEqual 0
        S3.deleteObject(defaultRegionBucket, sourceKey3).runWith(Sink.head).futureValue shouldEqual akka.Done
    }
  }

  it should "upload 2 files, delete all files in bucket" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey1)).runWith(Sink.head)
      upload2 <- source.runWith(S3.multipartUpload(defaultRegionBucket, sourceKey2)).runWith(Sink.head)
    } yield (upload1, upload2)

    whenReady(results) {
      case (upload1, upload2) =>
        upload1.bucket shouldEqual defaultRegionBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultRegionBucket
        upload2.key shouldEqual sourceKey2

        S3.deleteObjectsByPrefix(defaultRegionBucket, prefix = None).runWith(Sink.head).futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultRegionBucket, None).runFold(0)((result, _) => result + 1)
        numOfKeysForPrefix shouldEqual 0
    }
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

  val staticProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials(accessKey, secret)
  )

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
                                 |  endpoint-url = "$endpointUrl"
                                 |}
    """.stripMargin).withFallback(super.config())

  override def otherRegionSettings =
    S3Settings()
      .withCredentialsProvider(staticProvider)
      .withEndpointUrl(endpointUrl)
      .withPathStyleAccess(true)

  it should "properly set the endpointUrl" in {
    S3Settings().endpointUrl.value shouldEqual endpointUrl
  }
}

object MinioS3IntegrationSpec {
  val accessKey = "TESTKEY"
  val secret = "TESTSECRET"
  val endpointUrl = "http://localhost:9000"
}
