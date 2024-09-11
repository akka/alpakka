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
  CreateDirectory,
  CreateFile,
  DeleteDirectory,
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
    "create directory" in {
      val maybeObjectMetadata = FileService
        .createDirectory(directoryPath = defaultDirectoryPath, requestBuilder = CreateDirectory())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.isDefined shouldBe true
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0
    }

    "create file" in {
      val maybeObjectMetadata = FileService
        .createFile(objectPath = fileObjectPath,
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
          objectPath = fileObjectPath,
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
        .getFile(objectPath = fileObjectPath, GetFile())
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
          .getProperties(fileObjectPath, GetProperties())
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
          .getFile(fileObjectPath, GetFile().withRange(range))
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
        .clearRange(objectPath = fileObjectPath, requestBuilder = ClearFileRange(range))
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.get.contentLength shouldBe 0
    }

    "delete file" in {
      val maybeObjectMetadata =
        FileService
          .deleteFile(fileObjectPath, DeleteFile())
          .withAttributes(getDefaultAttributes)
          .runWith(Sink.head)
          .futureValue

      maybeObjectMetadata.get.contentLength shouldBe 0
    }

    "get file after delete" in {
      val maybeObjectMetadata =
        FileService
          .getProperties(fileObjectPath, GetProperties())
          .withAttributes(getDefaultAttributes)
          .toMat(Sink.head)(Keep.right)
          .run()
          .futureValue

      maybeObjectMetadata shouldBe empty
    }

    "test operations on multi level directory structure" in {
      val directoryName = "sub-directory"
      val directoryPath = s"$defaultDirectoryPath/$directoryName"
      val filePath = s"$directoryPath/$fileName"

      // create directory
      FileService
        .createDirectory(directoryPath, CreateDirectory())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue shouldBe defined

      // create file
      FileService
        .createFile(filePath, CreateFile(contentLength, ContentTypes.`text/plain(UTF-8)`))
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue shouldBe defined

      // update content
      FileService
        .updateRange(
          objectPath = filePath,
          requestBuilder = UpdateFileRange(ByteRange(0, contentLength - 1), ContentTypes.`text/plain(UTF-8)`),
          payload = Source.single(ByteString(sampleText))
        )
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue shouldBe defined

      // get file properties
      FileService
        .getProperties(filePath, GetProperties())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue
        .map(_.contentLength) shouldBe Some(sampleText.length)

      // delete file
      FileService
        .deleteFile(filePath, DeleteFile())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue shouldBe defined

      // delete directory
      FileService
        .deleteDirectory(directoryPath, DeleteDirectory())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue shouldBe defined
    }

    "delete directory" in {
      val maybeObjectMetadata = FileService
        .deleteDirectory(directoryPath = defaultDirectoryPath, requestBuilder = DeleteDirectory())
        .withAttributes(getDefaultAttributes)
        .runWith(Sink.head)
        .futureValue

      maybeObjectMetadata.isDefined shouldBe true
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0
    }
  }
}
