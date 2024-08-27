/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.azure.storage.scaladsl.StorageIntegrationSpec
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

class StorageTest extends StorageIntegrationSpec {

  override protected implicit val system: ActorSystem = ActorSystem("StorageSystem")

  "AzureStorage Blob connector" should {

    "create container" in {

      //#create-container
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] = BlobService.createContainer("my-container")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#create-container

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "put blob" in {

      //#put-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val sampleText = "some random text"
      val contentLength = sampleText.length
      val payload = Source.single(ByteString.fromString(sampleText))
      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.putBlob(
          objectPath = "my-container/my-blob.txt",
          contentType = ContentTypes.`text/plain(UTF-8)`,
          contentLength = contentLength,
          payload = payload
        )

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#put-blob

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "get blob" in {

      //#get-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = "my-container/my-blob.txt")

      val eventualText: Future[ByteString] = source.toMat(Sink.head)(Keep.right).run()
      //#get-blob

      val text = eventualText.futureValue

      // TODO: validate
      println(text)
    }

    "get blob range" in {

      //#get-blob-range
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        BlobService.getBlob(objectPath = "my-container/my-blob.txt", range = Some(ByteRange.Slice(2, 5)))

      val eventualText: Future[ByteString] = source.toMat(Sink.head)(Keep.right).run()
      //#get-blob-range

      val text = eventualText.futureValue

      // TODO: validate
      println(text)
    }

    "get blob properties" in {

      //#get-blob-properties
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.getProperties(objectPath = "my-container/my-blob.txt")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#get-blob-properties

      val maybeMetadata = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(maybeMetadata)
    }

    "delete blob" in {

      //#delete-blob
      import akka.stream.alpakka.azure.storage.scaladsl.BlobService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        BlobService.deleteBlob(objectPath = "my-container/my-blob.txt")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#delete-blob

      val maybeMetadata = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(maybeMetadata)
    }
  }

  "AzureStorage File connector" should {

    "create file" in {

      //#create-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.createFile(objectPath = "/my-directory/my-file.txt",
                               contentType = ContentTypes.`text/plain(UTF-8)`,
                               maxSize = 44)

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#create-file

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "update range" in {

      //#update-range
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val sampleText = "The quick brown fox jumps over the lazy dog."
      val payload = Source.single(ByteString.fromString(sampleText))
      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.updateRange(objectPath = "/my-directory/my-file.txt",
                                contentType = ContentTypes.`text/plain(UTF-8)`,
                                range = ByteRange.Slice(0, sampleText.length - 1),
                                payload = payload)

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#update-range

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "get file" in {

      //#get-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[ByteString, Future[ObjectMetadata]] =
        FileService.getFile(objectPath = "/my-directory/my-file.txt")

      val eventualText: Future[ByteString] = source.toMat(Sink.head)(Keep.right).run()
      //#get-file

      val text = eventualText.futureValue

      // TODO: validate
      println(text)
    }

    "get file properties" in {

      //#get-file-properties
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.getProperties(objectPath = "/my-directory/my-file.txt")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#get-file-properties

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "clear range" in {

      //#clear-range
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.clearRange(objectPath = "/my-directory/my-file.txt", range = ByteRange.Slice(5, 11))

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#clear-range

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }

    "delete file" in {

      //#delete-file
      import akka.stream.alpakka.azure.storage.scaladsl.FileService
      import akka.stream.alpakka.azure.storage.ObjectMetadata

      val source: Source[Option[ObjectMetadata], NotUsed] =
        FileService.deleteFile(objectPath = "/my-directory/my-file.txt")

      val eventualMaybeMetadata: Future[Option[ObjectMetadata]] = source.toMat(Sink.head)(Keep.right).run()
      //#delete-file

      val metaData = eventualMaybeMetadata.futureValue

      // TODO: validate
      println(metaData)
    }
  }
}
