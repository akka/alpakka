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
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

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

  it should "upload with real credentials" ignore {

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

  it should "download with real credentials" ignore {

    val download = defaultRegionClient.download(defaultRegionBucket, objectKey)

    val result = download.map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe objectValue
  }

  it should "upload and download with spaces in the key" ignore {
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

  it should "upload and download with brackets in the key" ignore {
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

  it should "upload and download with spaces in the key in non us-east-1 zone" ignore {
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

  it should "upload and download with brackets in the key in non us-east-1 zone" ignore {
    val objectKey = "abc/DEF/2017/06/15/1234 (1).TXT"
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
