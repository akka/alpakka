/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.azure.storage.requests.{
  ClearFileRange,
  CreateFile,
  DeleteFile,
  GetFile,
  GetProperties,
  UpdateFileRange
}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.scalatest.Ignore

@Ignore
class AzureIntegrationTest extends StorageIntegrationSpec {

  override protected implicit val system: ActorSystem = ActorSystem("AzureIntegrationTest")

  // Azurite doesn't support FileService yet so can't add these tests in the base class
  "FileService" should {
    "create file" in {
      val maybeObjectMetadata = FileService
        .createFile(objectPath = objectPath,
                    requestBuilder = CreateFile(contentLength, ContentTypes.`text/plain(UTF-8)`))
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.isDefined shouldBe true
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0
    }

    "put range" in {
      val maybeObjectMetadata = FileService
        .updateRange(
          objectPath = objectPath,
          requestBuilder = UpdateFileRange(ByteRange(0, contentLength - 1), ContentTypes.`text/plain(UTF-8)`),
          payload = Source.single(ByteString(sampleText))
        )
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.isDefined shouldBe true
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0
    }

    "get file" in {
      val (maybeEventualObjectMetadata, eventualText) = FileService
        .getFile(objectPath = objectPath, GetFile())
        .via(framing)
        .map(byteString => byteString.utf8String + System.lineSeparator())
        .toMat(Sink.seq)(Keep.both)
        .run()

      val objectMetadata = maybeEventualObjectMetadata.futureValue
      objectMetadata.contentType shouldBe Some(ContentTypes.`text/plain(UTF-8)`.value)
      objectMetadata.contentLength shouldBe sampleText.length
      eventualText.futureValue.mkString("") shouldBe sampleText
    }

    "get file properties" in {
      val maybeObjectMetadata =
        FileService
          .getProperties(objectPath, GetProperties())
          .withAttributes(getDefaultAttributes)
          .runWith(Sink.head)
          .futureValue

      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentType shouldBe Some(ContentTypes.`text/plain(UTF-8)`.value)
      objectMetadata.contentLength shouldBe sampleText.length
    }

    "get file range" in {
      val range = ByteRange(0, 8)
      val (maybeEventualObjectMetadata, eventualText) =
        FileService
          .getFile(objectPath, GetFile().withRange(range))
          .withAttributes(getDefaultAttributes)
          .via(framing)
          .map(_.utf8String)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val objectMetadata = maybeEventualObjectMetadata.futureValue
      objectMetadata.contentLength shouldBe (range.last - range.first + 1)
      eventualText.futureValue.mkString("") shouldBe "The quick"
    }

    "clear range" in {
      val range = ByteRange(16, 24)
      val maybeObjectMetadata = FileService
        .clearRange(objectPath = objectPath, requestBuilder = ClearFileRange(range))
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.get.contentLength shouldBe 0
    }

    "delete file" in {
      val maybeObjectMetadata =
        FileService
          .deleteFile(objectPath, DeleteFile())
          .withAttributes(getDefaultAttributes)
          .toMat(Sink.head)(Keep.right)
          .run()
          .futureValue

      maybeObjectMetadata.get.contentLength shouldBe 0
    }

    "get file after delete" in {
      val maybeObjectMetadata =
        FileService
          .getProperties(objectPath, GetProperties())
          .withAttributes(getDefaultAttributes)
          .toMat(Sink.head)(Keep.right)
          .run()
          .futureValue

      maybeObjectMetadata shouldBe empty
    }
  }
}
