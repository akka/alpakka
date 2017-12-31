/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.MetaHeaders
import akka.stream.alpakka.s3.{Proxy, S3Settings}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.regions.AwsRegionProvider
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore, Matchers}

/*
 * This is an integration test and ignored by default
 *
 * AWS:
 * For running the tests you need to create 2 buckets:
 *  - one in region us-east-1
 *  - one in an other region (eg eu-central-1)
 * Update the bucket name and regions in the code below
 *
 * Set your keys aws access-key-id and secret-access-key in src/test/resources/application.conf
 *
 * Bluemix: To run the tests create two buckets, leave the region field empty
 * since it is specified within the endpoint.Update the bucket name in the code
 * below.
 *
 * Set your bluemix keys access-key-id and secret-access-key in src/test/resources/application.conf
 *
 * Comment @ignore and run the tests
 * (tests that do listing counts might need some tweaking)
 *
 */
case class S3ConnectionProperties(
    name: String,
    defaultRegion: String,
    defaultRegionBucket: String,
    otherRegion: String,
    otherRegionBucket: String,
    settings: S3Settings,
    otherRegionSettings: S3Settings,
    defaultRegionClient: S3Client,
    otherRegionClient: S3Client,
    objectKey: String,
    objectValue: String,
    metaHeaders: Map[String, String]
)

@Ignore
class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  def createAWSConnectionProperties = {

    val defaultRegion = "us-east-1"
    val defaultRegionProvider = new AwsRegionProvider {
      def getRegion = defaultRegion
    }
    val defaultRegionBucket = "fad437a8-b37a-43c8-9fb8-eb18821b8d99"

    val otherRegion = "eu-central-1"
    val otherRegionProvider = new AwsRegionProvider {
      def getRegion = otherRegion
    }
    val otherRegionBucket = "fad437a8-b37a-43c8-9fb8-eb18821b8d95"

    val settings = S3Settings(ConfigFactory.load().getConfig("aws")).copy(s3RegionProvider = defaultRegionProvider)
    val otherRegionSettings = settings.copy(pathStyleAccess = true, s3RegionProvider = otherRegionProvider)

    val defaultRegionClient = new S3Client(settings)
    val otherRegionClient = new S3Client(otherRegionSettings)

    val objectKey = "test"

    val objectValue = "Some String"
    val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

    S3ConnectionProperties(
      "AWS",
      defaultRegion,
      defaultRegionBucket,
      otherRegion,
      otherRegionBucket,
      settings,
      otherRegionSettings,
      defaultRegionClient,
      otherRegionClient,
      objectKey,
      objectValue,
      metaHeaders
    )
  }

  def createBluemixConnectionProperties = {

    val defaultRegionBucket = "fad437a8-b37a-43c8-9fb8-eb18821b8d99"
    val otherRegionBucket = "fad437a8-b37a-43c8-9fb8-eb18821b8d99"
    val noRegionProvider = new AwsRegionProvider {
      def getRegion = ""
    }

    val settings = S3Settings(ConfigFactory.load().getConfig("bluemix")).copy(
      proxy = Some(Proxy(host = "s3.eu-geo.objectstorage.softlayer.net", port = 443, scheme = "https")),
      pathStyleAccess = true,
      s3RegionProvider = noRegionProvider
    )
    val otherRegionSettings = settings.copy(
      proxy = Some(Proxy(host = "s3.ams-eu-geo.objectstorage.softlayer.net", port = 443, scheme = "https")),
      pathStyleAccess = true,
      s3RegionProvider = noRegionProvider
    )

    val defaultRegionClient = new S3Client(settings)
    val otherRegionClient = new S3Client(otherRegionSettings)

    val objectKey = "test"

    val objectValue = "Some String"

    S3ConnectionProperties(
      "Bluemix",
      "",
      defaultRegionBucket,
      "",
      otherRegionBucket,
      settings,
      otherRegionSettings,
      defaultRegionClient,
      otherRegionClient,
      objectKey,
      objectValue,
      Map()
    )
  }

  List(createAWSConnectionProperties, createBluemixConnectionProperties).map { settings =>
    it should s"list with real credentials (${settings.name})" in {
      val result = settings.defaultRegionClient.listBucket(settings.defaultRegionBucket, None).runWith(Sink.seq)

      val listingResult = result.futureValue
      listingResult.size shouldBe 3
    }

    it should s"list with real credentials in other region (${settings.name})" in {
      val result = settings.otherRegionClient.listBucket(settings.otherRegionBucket, None).runWith(Sink.seq)

      val listingResult = result.futureValue
      listingResult.size shouldBe 3
    }

    it should s"upload with real credentials (${settings.name})" in {

      val source: Source[ByteString, Any] = Source(ByteString(settings.objectValue) :: Nil)
      //val source: Source[ByteString, Any] = FileIO.fromPath(Paths.get("/tmp/IMG_0470.JPG"))

      val result =
        source.runWith(
          settings.defaultRegionClient.multipartUpload(settings.defaultRegionBucket,
                                                       settings.objectKey,
                                                       metaHeaders = MetaHeaders(settings.metaHeaders))
        )

      val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
      multipartUploadResult.bucket shouldBe settings.defaultRegionBucket
      multipartUploadResult.key shouldBe settings.objectKey
    }

    it should s"download with real credentials (${settings.name})" in {

      val download = settings.defaultRegionClient.download(settings.defaultRegionBucket, settings.objectKey)

      val result = download.map(_.decodeString("utf8")).runWith(Sink.head)

      Await.ready(result, 5.seconds).futureValue shouldBe settings.objectValue
    }

    it should s"upload and download with spaces in the key (${settings.name})" in {
      val source: Source[ByteString, Any] = Source(ByteString(settings.objectValue) :: Nil)

      val results = for {
        upload <- source.runWith(
          settings.defaultRegionClient.multipartUpload(settings.defaultRegionBucket,
                                                       settings.objectKey,
                                                       metaHeaders = MetaHeaders(settings.metaHeaders))
        )
        download <- settings.defaultRegionClient
          .download(settings.defaultRegionBucket, settings.objectKey)
          .map(_.decodeString("utf8"))
          .runWith(Sink.head)
      } yield (upload, download)

      val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

      multipartUploadResult.bucket shouldBe settings.defaultRegionBucket
      multipartUploadResult.key shouldBe settings.objectKey
      downloaded shouldBe settings.objectValue
    }

    it should s"upload and download with brackets in the key (${settings.name})" in {
      val source: Source[ByteString, Any] = Source(ByteString(settings.objectValue) :: Nil)

      val results = for {
        upload <- source.runWith(
          settings.defaultRegionClient.multipartUpload(settings.defaultRegionBucket,
                                                       settings.objectKey,
                                                       metaHeaders = MetaHeaders(settings.metaHeaders))
        )
        download <- settings.defaultRegionClient
          .download(settings.defaultRegionBucket, settings.objectKey)
          .map(_.decodeString("utf8"))
          .runWith(Sink.head)
      } yield (upload, download)

      val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

      multipartUploadResult.bucket shouldBe settings.defaultRegionBucket
      multipartUploadResult.key shouldBe settings.objectKey
      downloaded shouldBe settings.objectValue
    }

    it should s"upload and download with spaces in the key in other region (${settings.name})" in {
      val source: Source[ByteString, Any] = Source(ByteString(settings.objectValue) :: Nil)

      val results = for {
        upload <- source.runWith(
          settings.otherRegionClient.multipartUpload(settings.otherRegionBucket,
                                                     settings.objectKey,
                                                     metaHeaders = MetaHeaders(settings.metaHeaders))
        )
        download <- settings.otherRegionClient
          .download(settings.otherRegionBucket, settings.objectKey)
          .map(_.decodeString("utf8"))
          .runWith(Sink.head)
      } yield (upload, download)

      val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

      multipartUploadResult.bucket shouldBe settings.otherRegionBucket
      multipartUploadResult.key shouldBe settings.objectKey
      downloaded shouldBe settings.objectValue
    }

    it should s"upload and download with special characters in the key in non other region (${settings.name})" in {
      val source: Source[ByteString, Any] = Source(ByteString(settings.objectValue) :: Nil)

      val results = for {
        upload <- source.runWith(
          settings.otherRegionClient.multipartUpload(settings.otherRegionBucket,
                                                     settings.objectKey,
                                                     metaHeaders = MetaHeaders(settings.metaHeaders))
        )
        download <- settings.otherRegionClient
          .download(settings.otherRegionBucket, settings.objectKey)
          .map(_.decodeString("utf8"))
          .runWith(Sink.head)
      } yield (upload, download)

      val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

      multipartUploadResult.bucket shouldBe settings.otherRegionBucket
      multipartUploadResult.key shouldBe settings.objectKey
      downloaded shouldBe settings.objectValue
    }
  }
}
