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

class B2StreamSpec extends AsyncFlatSpec with B2IntegrationTest {
  val thisRun = System.currentTimeMillis().toString

  val datas = (0 until 10) map { x =>
    s"$thisRun-file-$x" -> s"$thisRun-${UUID.randomUUID()}-data-$x"
  }

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val streams = new B2Streams(credentials)

  it should "upload then download" in pendingUntilFixed { // TODO: add "delete" in the end to clean up afterwards
    val upload = streams.uploadFiles(bucketId)
    val uploadedFiles = Source(datas)
      .map { case (fileName, data) =>
        UploadFileRequest(FileName(fileName), ByteString(data))
      }
      .via(upload)
      .map { x =>
        x.fileId -> x.fileName
      }
      .runWith(Sink.seq)

    val download = streams.downloadFilesById(bucketId)
    uploadedFiles flatMap { uploadedFiles =>
      val lookup = uploadedFiles.toMap
      lookup.size shouldEqual datas.size

      val downloaded = Source(lookup.keySet)
        .via(download)
        .map { x =>
          lookup(x.fileId) -> x.data.decodeString(StandardCharsets.UTF_8)
        }
        .runWith(Sink.seq)

      downloaded flatMap { downloaded =>
        downloaded.toSet shouldEqual datas.toSet
      }
    }
  }
}
