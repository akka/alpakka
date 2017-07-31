/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.impl.MetaHeaders
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

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
class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  val defaultRegion = "us-east-1"
  val defaultRegionBucket = "my-test-us-east-1"

  val otherRegion = "eu-central-1"
  val otherRegionBucket = "my.test.frankfurt" // with dots forcing path style access

  val settings = S3Settings(ConfigFactory.load()).copy(s3Region = defaultRegion)
  val otherRegionSettings = settings.copy(pathStyleAccess = true, s3Region = otherRegion)

  val defaultRegionClient = new S3Client(settings)
  val otherRegionClient = new S3Client(otherRegionSettings)

  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  it should "list with real credentials" in {
    val result = defaultRegionClient.listBucket(defaultRegionBucket, None).runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe 3
  }

  it should "list with real credentials in non us-east-1 zone" in {
    val result = otherRegionClient.listBucket(otherRegionBucket, None).runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe 2
  }

  it should "upload with real credentials" in {

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

    val result = download.map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe objectValue
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
    val objectKey = "føldęrü/1234()[]><!?: .TXT"
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
