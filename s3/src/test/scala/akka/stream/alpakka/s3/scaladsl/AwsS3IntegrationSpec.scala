/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.impl.{MetaHeaders, S3Headers}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

import com.amazonaws.regions.AwsRegionProvider

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
class AwsS3IntegrationSpec extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  val defaultRegion = "us-east-1"
  val defaultRegionProvider = new AwsRegionProvider {
    val getRegion: String = defaultRegion
  }
  val defaultRegionBucket = "my-test-us-east-1"

  val otherRegion = "eu-central-1"
  val otherRegionProvider = new AwsRegionProvider {
    val getRegion: String = otherRegion
  }
  val otherRegionBucket = "my.test.frankfurt" // with dots forcing path style access

  val settings = S3Settings(ConfigFactory.load().getConfig("aws")).copy(s3RegionProvider = defaultRegionProvider)
  val otherRegionSettings = settings.copy(pathStyleAccess = true, s3RegionProvider = otherRegionProvider)

  val defaultRegionClient = new S3Client(settings)
  val otherRegionClient = new S3Client(otherRegionSettings)

  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  it should "list with real credentials" in {
    val result = defaultRegionClient.listBucket(defaultRegionBucket, None).runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe 4
  }

  it should "list with real credentials in non us-east-1 zone" in {
    val result = otherRegionClient.listBucket(otherRegionBucket, None).runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe 5
  }

  it should "upload with real credentials" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result =
      defaultRegionClient.putObject(defaultRegionBucket,
                                    objectKey,
                                    data,
                                    bytes.length,
                                    s3Headers = S3Headers(MetaHeaders(metaHeaders)))

    val uploadResult = Await.ready(result, 90.seconds).futureValue
    uploadResult.eTag should not be empty
  }

  it should "upload and delete" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result = for {
      put <- defaultRegionClient.putObject(defaultRegionBucket,
                                           objectKey,
                                           data,
                                           bytes.length,
                                           s3Headers = S3Headers(MetaHeaders(metaHeaders)))
      metaBefore <- defaultRegionClient.getObjectMetadata(defaultRegionBucket, objectKey)
      delete <- defaultRegionClient.deleteObject(defaultRegionBucket, objectKey)
      metaAfter <- defaultRegionClient.getObjectMetadata(defaultRegionBucket, objectKey)
    } yield {
      (put, delete, metaBefore, metaAfter)
    }

    val (putResult, deleteResult, metaBefore, metaAfter) = Await.ready(result, 90.seconds).futureValue
    putResult.eTag should not be empty
    metaBefore should not be empty
    metaAfter shouldBe empty
  }

  it should "upload multipart with real credentials" in {

    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)
    //val source: Source[ByteString, Any] = FileIO.fromPath(Paths.get("/tmp/IMG_0470.JPG"))

    val result =
      source.runWith(
        defaultRegionClient.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
      )

    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" in {

    val download = defaultRegionClient.download(defaultRegionBucket, objectKey)

    val (metaFuture, bodyFuture) = download
      .map(_.decodeString("utf8"))
      .toMat(Sink.head)(Keep.both)
      .run()

    val result = for {
      body <- bodyFuture
      meta <- metaFuture
    } yield (body, meta)

    val (body, meta) = Await.ready(result, 5.seconds).futureValue
    body shouldBe objectValue
    meta.eTag should not be empty
  }

  it should "upload and download with spaces in the key" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(
        defaultRegionClient.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
      )
      download <- defaultRegionClient
        .download(defaultRegionBucket, objectKey)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue
  }

  it should "upload and download with brackets in the key" in {
    val objectKey = "abc/DEF/2017/06/15/1234 (1).TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(
        defaultRegionClient.multipartUpload(defaultRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
      )
      download <- defaultRegionClient
        .download(defaultRegionBucket, objectKey)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe defaultRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue
  }

  it should "upload and download with spaces in the key in non us-east-1 zone" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(
        otherRegionClient.multipartUpload(otherRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
      )
      download <- otherRegionClient
        .download(otherRegionBucket, objectKey)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe otherRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue
  }

  it should "upload and download with special characters in the key in non us-east-1 zone" in {
    // we want ASCII and other UTF-8 characters!
    val objectKey = "føldęrü/1234()[]><!? .TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source.runWith(
        otherRegionClient.multipartUpload(otherRegionBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
      )
      download <- otherRegionClient
        .download(otherRegionBucket, objectKey)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe otherRegionBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue
  }
}
