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

/**
 * Internal Api
 *
 * Splits up a byte stream source into sub-flows of a minimum size. Does not attempt to create chunks of an exact size.
 */
@InternalApi private[impl] object SplitAfterSize {
  def apply[I, M](minChunkSize: Long)(in: Flow[I, ByteString, M]): SubFlow[ByteString, M, in.Repr, in.Closed] =
    in.via(insertMarkers(minChunkSize)).splitWhen(_ == NewStream).collect { case bs: ByteString => bs }

  private case object NewStream

  private def insertMarkers(minChunkSize: Long) = new GraphStage[FlowShape[ByteString, Any]] {
    val in = Inlet[ByteString]("SplitAfterSize.in")
    val out = Outlet[Any]("SplitAfterSize.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with OutHandler with InHandler {
        var count: Long = 0
        override def onPull(): Unit = pull(in)

        override def onPush(): Unit = {
          val elem = grab(in)
          count += elem.size
          if (count >= minChunkSize) {
            count = 0
            emitMultiple(out, elem :: NewStream :: Nil)
          } else emit(out, elem)
        }
        setHandlers(in, out, this)
      }
  }
}
