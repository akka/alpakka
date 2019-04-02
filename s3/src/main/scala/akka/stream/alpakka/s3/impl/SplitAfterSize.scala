/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.annotation.InternalApi
import akka.stream.scaladsl.SubFlow
import akka.stream.stage.GraphStage
import akka.util.ByteString
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStageLogic
import akka.stream.Attributes
import akka.stream.stage.OutHandler
import akka.stream.stage.InHandler
import akka.stream.scaladsl.Flow

import scala.annotation.tailrec

/**
 * Internal Api
 *
 * Splits up a byte stream source into sub-flows of a minimum size. Does not attempt to create chunks of an exact size.
 */
@InternalApi private[impl] object SplitAfterSize {
  def apply[I, M](minChunkSize: Int,
                  maxChunkSize: Int)(in: Flow[I, ByteString, M]): SubFlow[ByteString, M, in.Repr, in.Closed] = {
    require(minChunkSize < maxChunkSize, "the min chunk size must be smaller than the max chunk size")
    in.via(insertMarkers(minChunkSize, maxChunkSize)).splitWhen(_ == NewStream).collect { case bs: ByteString => bs }
  }

  private case object NewStream

  private def insertMarkers(minChunkSize: Long, maxChunkSize: Int) = new GraphStage[FlowShape[ByteString, Any]] {
    val in = Inlet[ByteString]("SplitAfterSize.in")
    val out = Outlet[Any]("SplitAfterSize.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with OutHandler with InHandler {
        var count: Int = 0
        override def onPull(): Unit = pull(in)

        override def onPush(): Unit = {
          val elem = grab(in)
          count += elem.size
          if (count > maxChunkSize) {
            splitElement(elem, elem.size - (count - maxChunkSize))
          } else if (count >= minChunkSize) {
            count = 0
            emitMultiple(out, elem :: NewStream :: Nil)
          } else emit(out, elem)
        }

        @tailrec private def splitElement(elem: ByteString, splitPos: Int): Unit =
          if (elem.size > splitPos) {
            val (part1, rest) = elem.splitAt(splitPos)
            emitMultiple(out, part1 :: NewStream :: Nil)
            splitElement(rest, maxChunkSize)
          } else {
            count = elem.size
            emit(out, elem)
          }

        setHandlers(in, out, this)
      }
  }
}
