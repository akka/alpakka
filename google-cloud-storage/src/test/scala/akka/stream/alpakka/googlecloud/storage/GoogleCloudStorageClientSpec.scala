/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.util.UUID

import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.googlecloud.storage.Model.StorageObject
import akka.stream.alpakka.googlecloud.storage.Session.GoogleAuthConfiguration
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mutable.{After, Specification}

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class GoogleCloudStorageClientSpec(implicit ee: ExecutionEnv) extends Specification with WithMaterializerGlobal {

  "The client" should {

    "be able to list an empty bucket" in new WithConfiguredClient {
      skipped("the bucket is no longer empty")
      val objects = Await.result(googleCloudStorageClient
                                   .listBucket(bucket)
                                   .runWith(Sink.seq),
                                 10.seconds)
      objects should beEmpty
    }

    "get an empty list when listing a non existing folder" in new WithConfiguredClient {
      googleCloudStorageClient
        .listBucket(bucket, Some("non-existent"))
        .runWith(Sink.seq)
        .map { objects =>
          objects should beEmpty
        }
        .await(0, 10.seconds)
    }

    "be able to list an existing folder" in new WithConfiguredClient {
      (for {
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
        listing should have size 2
      }).await(0, 10.seconds)
    }

    "get metadata of an existing file" in new WithConfiguredClient {
      val content = ByteString("metadata file")
      Await.result(
        googleCloudStorageClient
          .upload(bucket, getFullFileName("metadata-file"), ContentTypes.`text/plain(UTF-8)`, content)
          .map { _ =>
            googleCloudStorageClient.getStorageObject(bucket, getFullFileName("metadata-file")).map { option =>
              option should beSome[StorageObject]
              option.map { so =>
                so.name must be equalTo getFullFileName("metadata-file")
                so.size must be equalTo content.size.toString
                so.contentType must beSome(ContentTypes.`text/plain(UTF-8)`.toString())
              }
            }
          },
        10.seconds
      )
    }

    "get none when asking metadata of non-existing file" in new WithConfiguredClient {
      googleCloudStorageClient
        .getStorageObject(bucket, getFullFileName("metadata-file"))
        .map { option =>
          option should beNone
        }
        .await(0, 10.seconds)
    }

    "be able to upload a file" in new WithConfiguredClient {
      val fileName = getFullFileName("test-file")
      Await.result(
        googleCloudStorageClient
          .upload(bucket,
                  fileName,
                  ContentTypes.`text/plain(UTF-8)`,
                  ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray))
          .map { so =>
            so.name must be equalTo fileName
            so.size must be equalTo "50000"
          },
        10.seconds
      )
      Await.result(googleCloudStorageClient.listBucket(bucket, Some(folderName)).runWith(Sink.seq), 10.second) should have size 1
    }

    "be able to download an existing file" in new WithConfiguredClient {
      val fileName = getFullFileName("test-file")
      val content = ByteString(Random.alphanumeric.take(50000).map(c => c.toByte).toArray)
      Await.result(
        googleCloudStorageClient
          .upload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, content)
          .map { _ =>
            googleCloudStorageClient.download(bucket, fileName).runWith(Sink.head).map { bs =>
              bs must be equalTo content
            }
          },
        10.seconds
      )
    }

    "get an empty Stream when downloading a non extisting file" in new WithConfiguredClient {
      val fileName = getFullFileName("non-existing-file")
      googleCloudStorageClient
        .download(bucket, fileName)
        .runWith(Sink.seq)
        .map { bs =>
          bs must beEmpty
        }
        .await(0, 10.seconds)
    }

    "get a single empty ByteString when downloading a non extisting file" in new WithConfiguredClient {
      val fileName = getFullFileName("non-existing-file")
      (for {
        _ <- googleCloudStorageClient.upload(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, ByteString.empty)
        res <- googleCloudStorageClient.download(bucket, fileName).runWith(Sink.seq)
      } yield {
        res must be equalTo Seq(ByteString.empty)
      }).await(0, 10.seconds)
    }

    "check if a file exists" in new WithConfiguredClient {
      Await.result(
        googleCloudStorageClient
          .exists(bucket, getFullFileName("testFileExists"))
          .map { fileExists =>
            fileExists should be equalTo false
          }
          .flatMap { _ =>
            googleCloudStorageClient
              .upload(bucket,
                      getFullFileName("testFileExists"),
                      ContentTypes.`text/plain(UTF-8)`,
                      ByteString("aaaaaabbbbb"))
              .map { _ =>
                googleCloudStorageClient.exists(bucket, getFullFileName("testFileExists")).map { fileExists =>
                  fileExists should be equalTo true
                }
              }
          },
        10.seconds
      )
    }

    "delete an existing file" in new WithConfiguredClient {
      Await.result(
        googleCloudStorageClient
          .upload(bucket,
                  getFullFileName("fileToDelete"),
                  ContentTypes.`text/plain(UTF-8)`,
                  ByteString("File content"))
          .map { _ =>
            googleCloudStorageClient.delete(bucket, getFullFileName("fileToDelete")).map { result =>
              result must be equalTo true
            }
          },
        10.seconds
      )
    }

    "delete an unexisting file should not give an error" in new WithConfiguredClient {
      Await.result(googleCloudStorageClient.delete(bucket, getFullFileName("non-existing-file-to-delete")).map {
        result =>
          result must be equalTo false
      }, 10.seconds)
    }

    "provide a sink to stream data to gcs" in new WithConfiguredClient {
      val fileName: String = getFullFileName("big-streaming-file")
      val sink =
        googleCloudStorageClient.createUploadSink(bucket, fileName, ContentTypes.`text/plain(UTF-8)`, 4 * 256 * 1024)

      Await.result(
        Source
          .fromIterator(
            () =>
              Iterator.fill[ByteString](10) {
                ByteString(Random.alphanumeric.take(1234567).map(c => c.toByte).toArray)
            }
          )
          .runWith(sink)
          .map { so =>
            so.name must be equalTo fileName
            so.size must be equalTo "12345670"
          },
        200.seconds
      )
    }
  }

  trait WithConfiguredClient extends After {
    val bucket = "mybucket-test"
    val folderName = classOf[GoogleCloudStorageClientSpec].getSimpleName + UUID.randomUUID().toString + "/"

    val config: Config = ConfigFactory.load()
    val googleConfig =
      (if (config
             .hasPath("google.serviceAccountFile") && !config.getString("google.serviceAccountFile").trim.isEmpty) {
         Some(GoogleAuthConfiguration(config.getString("google.serviceAccountFile")))
       } else {
         None
       }).getOrElse {
        skipped("SKIPPED: Google cloud storage serviceAccount not configured")
        throw new RuntimeException("Google cloud storage serviceAccount not configured")
      }

    val googleCloudStorageClient = GoogleCloudStorageClient(googleConfig)

    def getFullFileName(file: String) = folderName + file

    override def after: Any =
      Await.result(googleCloudStorageClient.deleteFolder(bucket, folderName).map(_ => ()), 10.second)

  }

}
