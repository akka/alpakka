/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

@akka.annotation.InternalApi
private[storage] final case class Chunk(bytes: ByteString, totalSize: Option[Long] = None) {
  def size: Int = bytes.size
}

// Inspired from akka doc : https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala#chunking-up-a-stream-of-bytestrings-into-limited-size-bytestrings
@akka.annotation.InternalApi
private[storage] class Chunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, Chunk]] {

  val in = Inlet[ByteString]("Chunker.in")
  val out = Outlet[Chunk]("Chunker.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val bufferBuilder = ByteString.newBuilder
    private var totalSize = 0l

    setHandler(out, new OutHandler {
      override def onPull(): Unit =
        emitChunk()
    })
    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          bufferBuilder.append(elem)
          emitChunk()
        }

        override def onUpstreamFinish(): Unit =
          if (bufferBuilder.isEmpty) completeStage()
          else {
            if (isAvailable(out)) emitChunk()
          }
      }
    )

    private def emitChunk(): Unit =
      if (isClosed(in)) {
        if (bufferBuilder.nonEmpty) {
          totalSize += bufferBuilder.length
          emit(out, Chunk(bufferBuilder.result(), Some(totalSize)))
          bufferBuilder.clear()
        }
        completeStage()
      } else {
        if (bufferBuilder.isEmpty) {
          pull(in)
        } else if (bufferBuilder.length > chunkSize) {
          val (chunk, nextBuffer) = bufferBuilder.result.splitAt(chunkSize)
          bufferBuilder.clear()
          bufferBuilder.append(nextBuffer)
          totalSize += chunk.size
          emit(out, Chunk(chunk))
        } else {
          pull(in)
        }
      }

  }
}
