/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
 * Internal Api
 *
 * Buffers the complete incoming stream into memory, which can then be read several times afterwards.
 *
 * The stage waits for the incoming stream containing a context to complete. After that, it emits a single Chunk item
 * on its output which contains the latest context at that point in time. The Chunk contains a `ByteString` source that
 * can be materialized multiple times, and the total size of the file.
 *
 * @param maxSize Maximum size to buffer
 */
@InternalApi private[impl] final class MemoryWithContext[C](maxSize: Int)
    extends GraphStage[FlowShape[(ByteString, C), (Chunk, immutable.Iterable[C])]] {
  val in = Inlet[(ByteString, C)]("MemoryBuffer.in")
  val out = Outlet[(Chunk, immutable.Iterable[C])]("MemoryBuffer.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var buffer = ByteString.empty
      private val contextBuffer: ListBuffer[C] = new ListBuffer[C]()

      override def onPull(): Unit = if (isClosed(in)) emit() else pull(in)

      override def onPush(): Unit = {
        val (elem, context) = grab(in)
        if (buffer.size + elem.size > maxSize) {
          failStage(new IllegalStateException("Buffer size of " + maxSize + " bytes exceeded."))
        } else {
          buffer ++= elem
          // This is a corner case where context can have a sentinel value of null which represents the initial empty
          // stream. We don't want to add null's into the final output
          if (context != null)
            contextBuffer.append(context)
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) emit()
        completeStage()
      }

      private def emit(): Unit = emit(out, (MemoryChunk(buffer), contextBuffer.toList), () => completeStage())

      setHandlers(in, out, this)
    }

}
