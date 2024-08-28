/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Attributes
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.wordspec.AnyWordSpecLike

import java.security.MessageDigest
import java.util.Base64
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait StorageIntegrationSpec
    extends AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with OptionValues
    with LogCapturing {

  protected val defaultContainerName = "test-container"
  protected val fileName = "sample-blob.txt"
  protected val sampleText: String = "The quick brown fox jumps over the lazy dog." + System.lineSeparator()
  protected val contentLength: Long = sampleText.length.toLong
  protected val objectPath = s"$defaultContainerName/$fileName"
  protected val framing: Flow[ByteString, ByteString, NotUsed] =
    Framing.delimiter(ByteString(System.lineSeparator()), 256, allowTruncation = true)

  protected implicit val system: ActorSystem
  protected implicit lazy val ec: ExecutionContext = system.dispatcher
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3.minutes, 100.millis)

  override protected def afterAll(): Unit =
    Http(system)
      .shutdownAllConnectionPools()
      .foreach(_ => TestKit.shutdownActorSystem(system))

  protected def getDefaultAttributes: Attributes = StorageAttributes.settings(StorageSettings())

  "BlobService" should {
    "put blob" in {
      val maybeObjectMetadata =
        BlobService
          .putBlockBlob(
            objectPath = objectPath,
            contentType = ContentTypes.`text/plain(UTF-8)`,
            contentLength = contentLength,
            payload = Source.single(ByteString(sampleText))
          )
          .withAttributes(getDefaultAttributes)
          .runWith(Sink.head)
          .futureValue

      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentMd5 shouldBe Some(calculateDigest(sampleText))
    }

    "get blob" in {
      val (maybeEventualObjectMetadata, eventualText) =
        BlobService
          .getBlob(objectPath)
          .withAttributes(getDefaultAttributes)
          .via(framing)
          .map(byteString => byteString.utf8String + System.lineSeparator())
          .toMat(Sink.seq)(Keep.both)
          .run()

      val objectMetadata = maybeEventualObjectMetadata.futureValue
      objectMetadata.contentMd5 shouldBe Some(calculateDigest(sampleText))
      objectMetadata.contentLength shouldBe sampleText.length
      eventualText.futureValue.mkString("") shouldBe sampleText
    }

    "get blob properties" in {
      val maybeObjectMetadata =
        BlobService
          .getProperties(objectPath)
          .withAttributes(getDefaultAttributes)
          .runWith(Sink.head)
          .futureValue

      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentMd5 shouldBe Some(calculateDigest(sampleText))
      objectMetadata.contentLength shouldBe sampleText.length
    }

    "get blob range" in {
      val range = ByteRange.Slice(0, 8)
      val (maybeEventualObjectMetadata, eventualText) =
        BlobService
          .getBlob(objectPath, Some(range))
          .withAttributes(getDefaultAttributes)
          .via(framing)
          .map(_.utf8String)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val objectMetadata = maybeEventualObjectMetadata.futureValue
      objectMetadata.contentLength shouldBe (range.last - range.first + 1)
      eventualText.futureValue.head shouldBe "The quick"
    }

    "delete blob" in {
      val maybeObjectMetadata =
        BlobService
          .deleteBlob(objectPath)
          .withAttributes(getDefaultAttributes)
          .toMat(Sink.head)(Keep.right)
          .run()
          .futureValue

      maybeObjectMetadata.get.contentLength shouldBe 0
    }

    "get blob after delete" in {
      val maybeObjectMetadata =
        BlobService
          .getProperties(objectPath)
          .withAttributes(getDefaultAttributes)
          .toMat(Sink.head)(Keep.right)
          .run()
          .futureValue

      maybeObjectMetadata shouldBe empty
    }
  }

  protected def createContainer(containerName: String): Future[Done] = {
    BlobService
      .createContainer(containerName)
      .withAttributes(getDefaultAttributes)
      .runWith(Sink.ignore)
  }

  protected def calculateDigest(text: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.update(text.getBytes)
    val bytes = digest.digest()
    Base64.getEncoder.encodeToString(bytes)
  }
}
