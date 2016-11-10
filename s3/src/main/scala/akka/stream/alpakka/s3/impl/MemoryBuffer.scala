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
import akka.stream.scaladsl.Source

/**
 * Buffers the complete incoming stream into memory, which can then be read several times afterwards.
 *
 * The stage waits for the incoming stream to complete. After that, it emits a single Chunk item on its output. The Chunk
 * contains a bytestream source that can be materialized multiple times, and the total size of the file.
 *
 * @param maximumSize Maximum size to buffer
 */
class MemoryBuffer(maxSize: Int) extends GraphStage[FlowShape[ByteString, Chunk]] {
  val in = Inlet[ByteString]("in")
  val out = Outlet[Chunk]("out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var buffer = ByteString.empty

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
        if (buffer.size + elem.size > maxSize) {
          failStage(new IllegalStateException("Buffer size of " + maxSize + " bytes exceeded."))
        } else {
          buffer ++= elem
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) {
          emit()
        }
        completeStage()
      }
    })

    def emit(): Unit = {
      emit(out, Chunk(Source.single(buffer), buffer.size), () => completeStage())
    }
  }

}