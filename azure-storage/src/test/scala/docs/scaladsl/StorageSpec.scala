/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.azure.storage.scaladsl.StorageWireMockBase
import akka.stream.alpakka.azure.storage.scaladsl.StorageWireMockBase.ETagRawValue
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, OptionValues}

import scala.concurrent.Future
import scala.concurrent.duration._

class StorageSpec
    extends StorageWireMockBase
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with ScalaFutures
    with OptionValues
    with LogCapturing {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3.minutes, 100.millis)

  override protected def afterEach(): Unit = mock.removeMappings()

  "AzureStorage Blob connector" should {

    "create container" in {
      mockCreateContainer()

      //#create-container
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] = BlobService.createContainer(containerName)

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#create-container

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0L
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "put block blob" ignore {
      mockPutBlockBlob()

      //#put-block-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putBlockBlob(
          objectPath = s"$containerName/$blobName",
          contentType = ContentTypes.`text/plain(UTF-8)`,
          contentLength = contentLength,
          payload = Source.single(ByteString(payload))
        )

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#put-block-blob

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe contentLength
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "put page blob" in {
      mockPutPageBlob()

      //#put-page-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putPageBlock(
          objectPath = s"$containerName/$blobName",
          contentType = ContentTypes.`text/plain(UTF-8)`,
          maxBlockSize = 512L,
          blobSequenceNumber = Some(0)
        )

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#put-page-blob

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0L
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "put append blob" in {
      mockPutAppendBlob()

      //#put-append-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putAppendBlock(
          objectPath = s"$containerName/$blobName",
          contentType = ContentTypes.`text/plain(UTF-8)`
        )

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#put-append-blob

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0L
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    "get blob" in {
      mockGetBlob()

      //#get-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName")

      val eventualText = source.toMat(Sink.seq)(Keep.right).run()
      //#get-blob

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get blob range" in {
      mockGetBlobWithRange()

      //#get-blob-range
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", range = Some(subRange))

      val eventualText: Future[Seq[ByteString]] = source.toMat(Sink.seq)(Keep.right).run()
      //#get-blob-range

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe "quick"
    }

    "get blob properties" in {
      mockGetBlobProperties();

      //#get-blob-properties
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.getProperties(objectPath = s"$containerName/$blobName")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#get-blob-properties

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe contentLength
      metadata.contentType shouldBe Some(ContentTypes.`text/plain(UTF-8)`.value)
    }

    "delete blob" in {
      mockDeleteBlob()

      //#delete-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.deleteBlob(objectPath = s"$containerName/$blobName")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#delete-blob

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }
  }

  "AzureStorage File connector" should {

    "create file" in {
      mockCreateFile()

      //#create-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.createFile(objectPath = s"$containerName/$blobName",
                               contentType = ContentTypes.`text/plain(UTF-8)`,
                               maxSize = contentLength)

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#create-file

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "update range" ignore {
      mockUpdateRange()

      //#update-range
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.updateRange(
          objectPath = s"$containerName/$blobName",
          contentType = ContentTypes.`text/plain(UTF-8)`,
          range = contentRange,
          payload = Source.single(ByteString(payload))
        )

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#update-range

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    "get file" in {
      mockGetBlob()

      //#get-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        FileService.getFile(objectPath = s"$containerName/$blobName")

      val eventualText: Future[Seq[ByteString]] = source.toMat(Sink.seq)(Keep.right).run()
      //#get-file

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get file properties" in {
      mockGetBlobProperties()

      //#get-file-properties
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.getProperties(objectPath = s"$containerName/$blobName")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#get-file-properties

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe contentLength
      metadata.contentType shouldBe Some(ContentTypes.`text/plain(UTF-8)`.value)
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "clear range" ignore {
      mockClearRange()

      //#clear-range
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.clearRange(objectPath = s"$containerName/$blobName", range = subRange)

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#clear-range

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }

    "delete file" in {
      mockDeleteBlob()

      //#delete-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.deleteFile(objectPath = s"$containerName/$blobName")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#delete-file

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }
  }
}
