package akka.stream.alpakka.kinesis.sink

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.concurrent.{Future, Promise}

/** A graph stage that prevents downstream completion from propagating up along
 * the flow, and drop incoming elements until upstream completion.
 */
private[sink] class DropOnClose[T](onDrop: T => Unit)
    extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Done]] {
  val in: Inlet[T] = Inlet("in")
  val out: Outlet[T] = Outlet("out")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val mat = Promise[Done]()

    val logic = new GraphStageLogic(shape) with InHandler with OutHandler {

      setHandlers(in, out, this)

      override def onPull(): Unit = if (!isClosed(in)) pull(in)

      override def onPush(): Unit = {
        val elem = grab(in)
        if (isAvailable(out)) push(out, elem)
        else if (isClosed(out)) {
          onDrop(elem)
          pull(in)
        }
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        if (!isClosed(in) && !hasBeenPulled(in)) pull(in)
      }

      override def onUpstreamFinish(): Unit = mat.trySuccess(Done)

      override def onUpstreamFailure(ex: Throwable): Unit = mat.tryFailure(ex)
    }

    (logic, mat.future)
  }
}
