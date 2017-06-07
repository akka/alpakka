/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.nio.charset.StandardCharsets
import java.util.UUID
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol.{FileName, UploadFileRequest}
import akka.stream.alpakka.backblazeb2.scaladsl.B2Streams
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.AsyncFlatSpec
import org.scalatest.Matchers._

class B2StreamIntegrationSpec extends AsyncFlatSpec with B2IntegrationTest {
  val thisRun = System.currentTimeMillis().toString

  val n = 10
  val datas = (0 until n) map { x =>
    s"$thisRun-file-$x" -> s"$thisRun-${UUID.randomUUID()}-data-$x"
  }

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val streams = new B2Streams(credentials)

  it should "upload then download then delete" in {
    val upload = streams.uploadFiles(bucketId)
    val uploadedFiles = Source(datas).map {
      case (fn, data) =>
        UploadFileRequest(FileName(fn), ByteString(data))
    }.via(upload)
      .map { x =>
        x.fileId -> x.fileName
      }
      .runWith(Sink.seq)

    val download = streams.downloadFilesById(bucketId)
    uploadedFiles flatMap { uploadedFiles =>
      val lookup = uploadedFiles.toMap
      lookup.size shouldEqual datas.size
      val fileIds = Source(lookup.keySet)

      val downloaded = fileIds.via(download).runWith(Sink.seq)

      downloaded flatMap { downloaded =>
        val downloadedData = downloaded.map { x =>
          lookup(x.fileId).value -> x.data.decodeString(StandardCharsets.UTF_8)
        }

        downloadedData.toSet shouldEqual datas.toSet

        val delete = streams.deleteFileVersions(bucketId)
        val fileVersions = downloaded.map { x =>
          x.fileVersion
        }

        val deleted = Source(fileVersions).via(delete).runWith(Sink.seq)

        deleted map { deleted =>
          deleted.toSet shouldEqual fileVersions.toSet
        }
      }
    }
  }
}
