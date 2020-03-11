/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

/**
 * Internal Api
 *
 * Buffers the complete incoming stream into memory, which can then be read several times afterwards.
 *
 * The stage waits for the incoming stream to complete. After that, it emits a single Chunk item on its output. The Chunk
 * contains a `ByteString` source that can be materialized multiple times, and the total size of the file.
 *
 * @param maxSize Maximum size to buffer
 */
@InternalApi private[impl] final class MemoryBuffer(maxSize: Int) extends GraphStage[FlowShape[ByteString, Chunk]] {
  val in = Inlet[ByteString]("MemoryBuffer.in")
  val out = Outlet[Chunk]("MemoryBuffer.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var buffer = ByteString.empty

      override def onPull(): Unit = if (isClosed(in)) emit() else pull(in)

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
        if (isAvailable(out)) emit()
        completeStage()
      }

      private def emit(): Unit = emit(out, Chunk(Source.single(buffer), buffer.size), () => completeStage())

      setHandlers(in, out, this)
    }

}
