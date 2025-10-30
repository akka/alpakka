/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.stream.alpakka.azure.storage.StorageException
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
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
      import akka.stream.alpakka.azure.storage.requests.CreateContainer

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.createContainer(containerName, CreateContainer())

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#create-container

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0L
      objectMetadata.eTag shouldBe Some(ETagRawValue)
    }

    "delete container" in {
      mockDeleteContainer()

      //#delete-container
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.DeleteContainer

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.deleteContainer(containerName, DeleteContainer())

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#delete-container

      val maybeObjectMetadata = eventualMaybeMetadata.futureValue
      maybeObjectMetadata shouldBe defined
      val objectMetadata = maybeObjectMetadata.get
      objectMetadata.contentLength shouldBe 0L
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    "put block blob" ignore {
      mockPutBlockBlob()

      //#put-block-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.PutBlockBlob

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putBlockBlob(
          objectPath = s"$containerName/$blobName",
          payload = Source.single(ByteString(payload)),
          requestBuilder = PutBlockBlob(contentLength, ContentTypes.`text/plain(UTF-8)`)
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
    "put page blob" ignore {
      mockPutPageBlob()

      //#put-page-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.PutPageBlock

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putPageBlock(
          objectPath = s"$containerName/$blobName",
          requestBuilder = PutPageBlock(512L, ContentTypes.`text/plain(UTF-8)`).withBlobSequenceNumber(0)
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
    "put append blob" ignore {
      mockPutAppendBlob()

      //#put-append-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.PutAppendBlock

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putAppendBlock(objectPath = s"$containerName/$blobName",
                                   requestBuilder = PutAppendBlock(ContentTypes.`text/plain(UTF-8)`))

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
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", GetBlob())

      val eventualText = source.toMat(Sink.seq)(Keep.right).run()
      //#get-blob

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get blob with versionId" in {
      mockGetBlob(Some("versionId"))

      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", GetBlob().withVersionId("versionId"))

      val eventualText = source.toMat(Sink.seq)(Keep.right).run()

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get blob with optional header" in {
      mockGetBlob(leaseId = Some("leaseId"))

      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", GetBlob().withLeaseId("leaseId"))

      val eventualText = source.toMat(Sink.seq)(Keep.right).run()

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get blob with ServerSideEncryption" in {
      mockGetBlobWithServerSideEncryption()

      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName",
                            GetBlob().withServerSideEncryption(ServerSideEncryption.customerKey("SGVsbG9Xb3JsZA==")))

      val eventualText = source.toMat(Sink.seq)(Keep.right).run()

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get blob from non-existing container" in {
      mock404s()

      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", GetBlob())

      val eventualMetadata = source.toMat(Sink.seq)(Keep.right).run()
      eventualMetadata.failed.futureValue shouldBe
      StorageException(
        statusCode = StatusCodes.NotFound,
        errorCode = "ResourceNotFound",
        errorMessage = "The specified resource doesn't exist.",
        resourceName = None,
        resourceValue = None,
        reason = None
      )
    }

    "get blob range" in {
      mockGetBlobWithRange()

      //#get-blob-range
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetBlob

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = s"$containerName/$blobName", requestBuilder = GetBlob().withRange(subRange))

      val eventualText: Future[Seq[ByteString]] = source.toMat(Sink.seq)(Keep.right).run()
      //#get-blob-range

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe "quick"
    }

    "get blob properties" in {
      mockGetBlobProperties();

      //#get-blob-properties
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetProperties

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.getProperties(objectPath = s"$containerName/$blobName", GetProperties())

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
      import akka.stream.alpakka.azure.storage.requests.DeleteBlob

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.deleteBlob(objectPath = s"$containerName/$blobName", DeleteBlob())

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

    "create directory" in {
      mockCreateDirectory()

      //#create-directory
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.CreateDirectory

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.createDirectory(directoryPath = containerName, requestBuilder = CreateDirectory())

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#create-directory

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }

    "create file" in {
      mockCreateFile()

      //#create-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.CreateFile

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.createFile(objectPath = s"$containerName/$blobName",
                               requestBuilder = CreateFile(contentLength, ContentTypes.`text/plain(UTF-8)`))

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
      import akka.stream.alpakka.azure.storage.requests.UpdateFileRange

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.updateRange(
          objectPath = s"$containerName/$blobName",
          payload = Source.single(ByteString(payload)),
          requestBuilder = UpdateFileRange(contentRange, ContentTypes.`text/plain(UTF-8)`)
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
      import akka.stream.alpakka.azure.storage.requests.GetFile

      val source: Source[ByteString, Future[ObjectMetadata]] =
        FileService.getFile(objectPath = s"$containerName/$blobName", GetFile())

      val eventualText: Future[Seq[ByteString]] = source.toMat(Sink.seq)(Keep.right).run()
      //#get-file

      eventualText.futureValue.map(_.utf8String).mkString("") shouldBe payload
    }

    "get file properties" in {
      mockGetBlobProperties()

      //#get-file-properties
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.GetProperties

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.getProperties(objectPath = s"$containerName/$blobName", GetProperties())

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
      import akka.stream.alpakka.azure.storage.requests.ClearFileRange

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.clearRange(objectPath = s"$containerName/$blobName", requestBuilder = ClearFileRange(subRange))

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
      import akka.stream.alpakka.azure.storage.requests.DeleteFile

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.deleteFile(objectPath = s"$containerName/$blobName", DeleteFile())

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#delete-file

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.eTag shouldBe Some(ETagRawValue)
      metadata.contentLength shouldBe 0L
    }

    "delete directory" in {
      mockDeleteDirectory()

      //#delete-directory
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata
      import akka.stream.alpakka.azure.storage.requests.DeleteDirectory

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.deleteDirectory(directoryPath = containerName, requestBuilder = DeleteDirectory())

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.runWith(Sink.head)
      //#delete-directory

      val maybeMetadata = eventualMaybeMetadata.futureValue
      maybeMetadata shouldBe defined
      val metadata = maybeMetadata.get
      metadata.contentLength shouldBe 0L
    }
  }
}
