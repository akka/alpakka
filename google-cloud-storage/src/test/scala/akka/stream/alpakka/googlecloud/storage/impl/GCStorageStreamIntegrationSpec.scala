/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import java.util.UUID

import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.googlecloud.storage.WithMaterializerGlobal
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import akka.stream.alpakka.googlecloud.storage.GCStorageSettings
import scala.concurrent.Future

/**
 * USAGE
 * - Create a google cloud service account
 * - Make sure it has these roles:
 *    storage object creator
 *    storage object viewer
 *    storage object admin
 *    storage admin (to run the create/delete bucket test)
 * - modify test/resources/application.conf
 * - create a `alpakka` bucket for testing
 * - create a rewrite `alpakka-rewrite` bucket for testing
 */
class GCStorageStreamIntegrationSpec
    extends WordSpec
    with WithMaterializerGlobal
    with BeforeAndAfter
    with Matchers
    with ScalaFutures {

  private implicit val defaultPatience =
    PatienceConfig(timeout = 60.seconds, interval = 60.millis)

  var folderName: String = _

  def testFileName(file: String): String = folderName + file

  def settings: GCStorageSettings = GCStorageSettings()

  def bucket = "alpakka"
  def rewriteBucket = "alpakka-rewrite"
  def projectId = settings.projectId
  def clientEmail = settings.clientEmail
  def privateKey = settings.privateKey

  before {
    folderName = classOf[GCStorageStreamIntegrationSpec].getSimpleName + UUID.randomUUID().toString + "/"
  }

  after {
    Await.result(GCStorageStream.deleteObjectsByPrefixSource(bucket, Some(folderName)).runWith(Sink.seq), 10.seconds)
  }

  "GCStorageStream" should {

    "be able to create and delete a bucket" ignore {
      val randomBucketName = s"alpakka_${UUID.randomUUID().toString}"

      val res = for {
        bucket <- GCStorageStream.createBucketSource(randomBucketName, "europe-west1").runWith(Sink.head)
        afterCreate <- GCStorageStream.getBucketSource(bucket.name).runWith(Sink.head)
        _ <- GCStorageStream.deleteBucketSource(bucket.name).runWith(Sink.head)
        afterDelete <- GCStorageStream.getBucketSource(bucket.name).runWith(Sink.head)
      } yield (bucket, afterCreate, afterDelete)

      val (bucket, afterCreate, afterDelete) = res.futureValue
      bucket.kind shouldBe "storage#bucket"
      afterCreate.isDefined shouldBe true
      afterDelete shouldBe None
    }

    "be able to get bucket info" ignore {
      // the bucket is no longer empty
      val bucketInfo = GCStorageStream
        .getBucketSource(bucket)
        .runWith(Sink.head)
      bucketInfo.futureValue.map(_.kind) shouldBe Some("storage#bucket")
    }

    "be able to list an empty bucket" ignore {
      // the bucket is no longer empty
      val objects = GCStorageStream
        .listBucket(bucket, None)
        .runWith(Sink.seq)
      objects.futureValue shouldBe empty
    }

    "get an empty list when listing a non existing folder" ignore {
      val objects = GCStorageStream
        .listBucket(bucket, Some("non-existent"))
        .runWith(Sink.seq)

      objects.futureValue shouldBe empty
    }

    "be able to list an existing folder" ignore {
      val listing = for {
        _ <- GCStorageStream
          .putObject(bucket,
                     testFileName("testa.txt"),
                     Source.single(ByteString("testa")),
                     ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        _ <- GCStorageStream
          .putObject(bucket,
                     testFileName("testb.txt"),
                     Source.single(ByteString("testa")),
                     ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        listing <- GCStorageStream.listBucket(bucket, Some(folderName)).runWith(Sink.seq)
      } yield {
        listing
      }

      listing.futureValue should have size 2
    }

    "get metadata of an existing file" ignore {
      val content = ByteString("metadata file")

      val option = for {
        _ <- GCStorageStream
          .putObject(bucket, testFileName("metadata-file"), Source.single(content), ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        option <- GCStorageStream.getObject(bucket, testFileName("metadata-file")).runWith(Sink.head)
      } yield option

      val so = option.futureValue.get
      so.name shouldBe testFileName("metadata-file")
      so.size shouldBe content.size
      so.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
    }

    "get none when asking metadata of non-existing file" ignore {
      val option = GCStorageStream.getObject(bucket, testFileName("metadata-file")).runWith(Sink.head)
      option.futureValue shouldBe None
    }

    "be able to upload a file" ignore {
      val fileName = testFileName("test-file")
      val res = for {
        so <- GCStorageStream
          .putObject(
            bucket,
            fileName,
            Source.single(ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray)),
            ContentTypes.`text/plain(UTF-8)`
          )
          .runWith(Sink.head)
        listing <- GCStorageStream.listBucket(bucket, Some(folderName)).runWith(Sink.seq)
      } yield (so, listing)

      val (so, listing) = res.futureValue

      so.name shouldBe fileName
      so.size shouldBe 50000
      listing should have size 1
    }

    "be able to download an existing file" ignore {
      val fileName = testFileName("test-file")
      val content = ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray)
      val bs = for {
        _ <- GCStorageStream
          .putObject(bucket, fileName, Source.single(content), ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        bs <- GCStorageStream
          .download(bucket, fileName)
          .runWith(Sink.head)
          .flatMap(
            _.map(_.runWith(Sink.fold(ByteString.empty) { _ ++ _ })).getOrElse(Future.successful(ByteString.empty))
          )
      } yield bs
      bs.futureValue shouldBe content
    }

    "get a None when downloading a non extisting file" ignore {
      val fileName = testFileName("non-existing-file")
      val download = GCStorageStream
        .download(bucket, fileName)
        .runWith(Sink.head)
        .futureValue

      download shouldBe None
    }

    "get a single empty ByteString when downloading a non existing file" ignore {
      val fileName = testFileName("non-existing-file")
      val res = for {
        _ <- GCStorageStream
          .putObject(bucket, fileName, Source.single(ByteString.empty), ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        res <- GCStorageStream
          .download(bucket, fileName)
          .runWith(Sink.head)
          .flatMap(
            _.map(_.runWith(Sink.fold(ByteString.empty) { _ ++ _ })).getOrElse(Future.successful(ByteString.empty))
          )
      } yield res
      res.futureValue shouldBe ByteString.empty
    }

    "delete an existing file" ignore {
      val result = for {
        _ <- GCStorageStream
          .putObject(bucket,
                     testFileName("fileToDelete"),
                     Source.single(ByteString("File content")),
                     ContentTypes.`text/plain(UTF-8)`)
          .runWith(Sink.head)
        result <- GCStorageStream.deleteObjectSource(bucket, testFileName("fileToDelete")).runWith(Sink.head)
      } yield result
      result.futureValue shouldBe true
    }

    "delete an unexisting file should not give an error" ignore {
      val result =
        GCStorageStream.deleteObjectSource(bucket, testFileName("non-existing-file-to-delete")).runWith(Sink.head)
      result.futureValue shouldBe false
    }

    "provide a sink to stream data to gcs" ignore {
      val fileName = testFileName("big-streaming-file")
      val sink =
        GCStorageStream.resumableUpload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, 4 * 256 * 1024)

      val res = Source
        .fromIterator(
          () =>
            Iterator.fill[ByteString](10) {
              ByteString(Random.alphanumeric.take(1234567).map(c => c.toByte).toArray)
            }
        )
        .runWith(sink)

      val so = res.futureValue
      so.name shouldBe fileName
      so.size shouldBe 12345670
    }

    "rewrite file from source to destination path" ignore {
      val fileName = "big-streaming-file"

      val sink =
        GCStorageStream.resumableUpload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, 4 * 256 * 1024)

      val uploadResult = Source
        .fromIterator(
          () =>
            Iterator.fill[ByteString](10) {
              ByteString(Random.alphanumeric.take(1234567).map(c => c.toByte).toArray)
            }
        )
        .runWith(sink)

      val res = for {
        _ <- uploadResult
        res <- GCStorageStream.rewrite(bucket, fileName, rewriteBucket, fileName).run()
      } yield res

      res.futureValue.name shouldBe fileName
      res.futureValue.bucket shouldBe rewriteBucket
      GCStorageStream.getObject(rewriteBucket, fileName).runWith(Sink.head).futureValue.isDefined shouldBe true

      GCStorageStream.deleteObjectSource(bucket, fileName).runWith(Sink.head).futureValue
      GCStorageStream.deleteObjectSource(rewriteBucket, fileName).runWith(Sink.head).futureValue
    }
  }
}
