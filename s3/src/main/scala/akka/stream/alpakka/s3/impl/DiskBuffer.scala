package akka.stream.alpakka.s3.impl

import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.FileIO
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import java.nio.file.Path

/**
 * Buffers the complete incoming stream into a file, which can then be read several times afterwards.
 *
 * The stage waits for the incoming stream to complete. After that, it emits a single Chunk item on its output. The Chunk
 * contains a bytestream source that can be materialized multiple times, and the total size of the file.
 *
 * @param maxMaterializations Number of expected materializations for the completed chunk. After this, the temp file is deleted.
 * @param maximumSize Maximum size on disk to buffer
 */
class DiskBuffer(maxMaterializations: Int, maxSize: Int, tempPath: Option[Path]) extends GraphStage[FlowShape[ByteString, Chunk]] {
  if (maxMaterializations < 1) throw new IllegalArgumentException("maxMaterializations should be at least 1")
  if (maxSize < 1) throw new IllegalArgumentException("maximumSize should be at least 1")

  val in = Inlet[ByteString]("in")
  val out = Outlet[Chunk]("out")
  override val shape = FlowShape.of(in, out)

  override def initialAttributes = ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val path: Path = tempPath.map(dir => Files.createTempFile(dir, "s3-buffer-", ".bin")).getOrElse(Files.createTempFile("s3-buffer-", ".bin"))
    path.toFile().deleteOnExit()
    val writeBuffer = new RandomAccessFile(path.toFile(), "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, maxSize)
    var length = 0

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (isClosed(in)) {
          emit()
        } else {
          pull(in)
        }
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        length += elem.size
        writeBuffer.put(elem.asByteBuffer)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) {
          emit()
        }
        completeStage()
      }
    })

    private def emit(): Unit = {
      // TODO Should we do http://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java ?
      writeBuffer.force()

      val ch = new FileOutputStream(path.toFile(), true).getChannel()
      try {
        ch.truncate(length)
      } finally {
        ch.close()
      }

      val deleteCounter = new AtomicInteger(maxMaterializations)
      val src = FileIO.fromPath(path, 65536).mapMaterializedValue { f =>
        if (deleteCounter.decrementAndGet() <= 0) {
          import scala.concurrent.ExecutionContext.Implicits.global
          f.onComplete { _ =>
            path.toFile().delete()
          }
        }
        NotUsed
      }
      emit(out, Chunk(src, length), () => completeStage())
    }
  }
}
