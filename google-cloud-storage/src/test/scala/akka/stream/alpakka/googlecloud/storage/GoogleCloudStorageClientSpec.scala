/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import org.scalactic.source
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class GoogleCloudStorageClientSpec
    extends WordSpec
    with WithMaterializerGlobal
    with Matchers
    with ScalaFutures
    with WithConfiguredClient {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(10, Millis))

  "The client" should {

    "be able to list an empty bucket" ignoreIfConfigMissing {
      // the bucket is no longer empty
      val objects = googleCloudStorageClient
        .listBucket(bucket)
        .runWith(Sink.seq)
      objects.futureValue shouldBe empty
    }

    "get an empty list when listing a non existing folder" ignoreIfConfigMissing {
      val objects = googleCloudStorageClient
        .listBucket(bucket, Some("non-existent"))
        .runWith(Sink.seq)

      objects.futureValue shouldBe empty
    }

    "be able to list an existing folder" ignoreIfConfigMissing {
      val listing = for {
        _ <- googleCloudStorageClient.upload(bucket,
                                             getFullFileName("testa.txt"),
                                             ContentTypes.`text/plain(UTF-8)`,
                                             ByteString("testa"))
        _ <- googleCloudStorageClient.upload(bucket,
                                             getFullFileName("testb.txt"),
                                             ContentTypes.`text/plain(UTF-8)`,
                                             ByteString("testb"))
        listing <- googleCloudStorageClient.listBucket(bucket, Some(folderName)).runWith(Sink.seq)
      } yield {
        listing
      }

      listing.futureValue should have size 2
    }

    "get metadata of an existing file" ignoreIfConfigMissing {
      val content = ByteString("metadata file")

      val option = for {
        _ <- googleCloudStorageClient
          .upload(bucket, getFullFileName("metadata-file"), ContentTypes.`text/plain(UTF-8)`, content)
        option <- googleCloudStorageClient.getStorageObject(bucket, getFullFileName("metadata-file"))
      } yield option

      val so = option.futureValue.get
      so.name shouldBe getFullFileName("metadata-file")
      so.size shouldBe content.size.toString
      so.contentType shouldBe Some(ContentTypes.`text/plain(UTF-8)`.toString())
    }

    "get none when asking metadata of non-existing file" ignoreIfConfigMissing {
      val option = googleCloudStorageClient.getStorageObject(bucket, getFullFileName("metadata-file"))
      option.futureValue shouldBe None
    }

    "be able to upload a file" ignoreIfConfigMissing {
      val fileName = getFullFileName("test-file")
      val res = for {
        so <- googleCloudStorageClient.upload(bucket,
                                              fileName,
                                              ContentTypes.`text/plain(UTF-8)`,
                                              ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray))
        listing <- googleCloudStorageClient.listBucket(bucket, Some(folderName)).runWith(Sink.seq)
      } yield (so, listing)

      val (so, listing) = res.futureValue

      so.name shouldBe fileName
      so.size shouldBe "50000"
      listing should have size 1
    }

    "be able to download an existing file" ignoreIfConfigMissing {
      val fileName = getFullFileName("test-file")
      val content = ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray)
      val bs = for {
        _ <- googleCloudStorageClient.upload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, content)
        bs <- googleCloudStorageClient.download(bucket, fileName).runWith(Sink.fold(ByteString.empty) { _ ++ _ })
      } yield bs
      bs.futureValue shouldBe content
    }

    "get an empty Stream when downloading a non extisting file" ignoreIfConfigMissing {
      val fileName = getFullFileName("non-existing-file")
      val download = googleCloudStorageClient
        .download(bucket, fileName)
        .runWith(Sink.seq)
      download.futureValue shouldBe empty
    }

    "get a single empty ByteString when downloading a non extisting file" ignoreIfConfigMissing {
      val fileName = getFullFileName("non-existing-file")
      val res = for {
        _ <- googleCloudStorageClient.upload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, ByteString.empty)
        res <- googleCloudStorageClient.download(bucket, fileName).runWith(Sink.seq)
      } yield res
      res.futureValue shouldBe Seq(ByteString.empty)
    }

    "check if a file exists" ignoreIfConfigMissing {

      val res = for {
        before <- googleCloudStorageClient.exists(bucket, getFullFileName("testFileExists"))
        _ <- googleCloudStorageClient
          .upload(bucket,
                  getFullFileName("testFileExists"),
                  ContentTypes.`text/plain(UTF-8)`,
                  ByteString("aaaaaabbbbb"))
        after <- googleCloudStorageClient.exists(bucket, getFullFileName("testFileExists"))
      } yield (before, after)
      res.futureValue shouldBe ((false, true))
    }

    "delete an existing file" ignoreIfConfigMissing {
      val result = for {
        _ <- googleCloudStorageClient.upload(bucket,
                                             getFullFileName("fileToDelete"),
                                             ContentTypes.`text/plain(UTF-8)`,
                                             ByteString("File content"))
        result <- googleCloudStorageClient.delete(bucket, getFullFileName("fileToDelete"))
      } yield result
      result.futureValue shouldBe true
    }

    "delete an unexisting file should not give an error" ignoreIfConfigMissing {
      val result = googleCloudStorageClient.delete(bucket, getFullFileName("non-existing-file-to-delete"))
      result.futureValue shouldBe false
    }

    "provide a sink to stream data to gcs" ignoreIfConfigMissing {
      val fileName: String = getFullFileName("big-streaming-file")
      val sink =
        googleCloudStorageClient.createUploadSink(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, 4 * 256 * 1024)

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
      so.size shouldBe "12345670"
    }
  }

}

trait WithConfiguredClient extends WordSpec with BeforeAndAfter with Matchers {

  def actorSystem: ActorSystem
  def materializer: Materializer

  var folderName: String = _
  var googleCloudStorageClient: GoogleCloudStorageClient = _

  def getFullFileName(file: String) = folderName + file

  val config: Config = ConfigFactory.load()
  val googleConfig = GoogleAuthConfiguration(config)
  val bucket = config.getString("alpakka.test.google.bucket")

  implicit class StringExt(wordSpecStringWrapper: String) {
    def ignoreIfConfigMissing(f: => Any /* Assertion */ )(implicit pos: source.Position): Unit =
      if (googleConfig.isEmpty) {
        wordSpecStringWrapper.ignore(f)
      } else {
        wordSpecStringWrapper.in(f)
      }
  }

  before {
    folderName = classOf[GoogleCloudStorageClientSpec].getSimpleName + UUID.randomUUID().toString + "/"
    googleCloudStorageClient = GoogleCloudStorageClient(googleConfig.get)(actorSystem, materializer)
  }

  after {
    Await.result(googleCloudStorageClient.deleteFolder(bucket, folderName), 10.second)
  }

}
