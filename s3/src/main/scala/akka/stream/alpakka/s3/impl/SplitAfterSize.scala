package akka.stream.alpakka.s3.impl

import akka.stream.scaladsl.SubFlow
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.util.ByteString
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStageLogic
import akka.stream.Attributes
import akka.stream.stage.OutHandler
import akka.stream.stage.InHandler
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Flow

/**
 * Splits up a bytestream source into sub-flows of a minimum size. Does not attempt to create chunkes of an exact size.
 */
object SplitAfterSize {
  def apply[I, M](minChunkSize: Long)(in: Flow[I, ByteString, M]): SubFlow[ByteString, M, in.Repr, in.Closed] = {
    in.via(insertMarkers(minChunkSize)).splitWhen(_ == NewStream).filter(_.isInstanceOf[ByteString]).map(_.asInstanceOf[ByteString])
  }

  private case object NewStream

  private def insertMarkers(minChunkSize: Long) = new GraphStage[FlowShape[ByteString, Any]] {
    val in = Inlet[ByteString]("in")
    val out = Outlet[Any]("out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var count: Long = 0;

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          count += elem.size
          if (count >= minChunkSize) {
            println("*** split after " + count)
            count = 0
            emitMultiple(out, elem :: NewStream :: Nil)
          } else {
            emit(out, elem)
          }
        }

        override def onUpstreamFinish(): Unit = {
          println("*** complete after " + count)
          completeStage()
        }
      })

    }
  }
}