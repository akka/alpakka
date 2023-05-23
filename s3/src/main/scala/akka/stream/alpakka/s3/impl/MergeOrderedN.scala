package akka.stream.alpakka.s3.impl

import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet, UniformFanInShape}

import scala.collection.{immutable, mutable}

@InternalApi private[impl] object MergeOrderedN {
  /** @see [[MergeOrderedN]] */
  def apply[T](inputPorts: Int, breadth: Int) =
    new MergeOrderedN[T](inputPorts, breadth)
}

/**
  * Takes multiple streams (in ascending order of input ports) whose elements will be pushed only if all elements from the
  * previous stream(s) are already pushed downstream.
  *
  * The `breadth` controls how many upstream are pulled in parallel.
  * That means elements might be received in any order, but will be buffered (if necessary) until their time comes.
  *
  * '''Emits when''' the next element from upstream (in ascending order of input ports) is available
  *
  * '''Backpressures when''' downstream backpressures
  *
  * '''Completes when''' all upstreams complete and there are no more buffered elements
  *
  * '''Cancels when''' downstream cancels
  */
@InternalApi private[impl] final class MergeOrderedN[T](val inputPorts: Int, val breadth: Int) extends GraphStage[UniformFanInShape[T, T]] {
  require(inputPorts > 1, "input ports must be > 1")
  require(breadth > 0, "breadth must be > 0")

  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("MergeOrderedN.in" + i))
  val out: Outlet[T] = Outlet[T]("MergeOrderedN.out")
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
    private val bufferByInPort = mutable.Map.empty[Int, mutable.Queue[T]] // Queue must not be empty, if so entry should be removed
    private var currentHeadInPortIdx = 0
    private var currentLastInPortIdx = 0
    private val overallLastInPortIdx = inputPorts - 1

    setHandler(out, this)

    in.zipWithIndex.foreach { case (inPort, idx) =>
      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(inPort)
          if (currentHeadInPortIdx != idx || !isAvailable(out)) {
            bufferByInPort.updateWith(idx) {
              case Some(inPortBuffer) =>
                Some(inPortBuffer.enqueue(elem))
              case None =>
                val inPortBuffer = mutable.Queue.empty[T]
                inPortBuffer.enqueue(elem)
                Some(inPortBuffer)
            }
          } else {
            pushUsingQueue(Some(elem))
          }
          tryPull(inPort)
        }

        override def onUpstreamFinish(): Unit = {
          if (canCompleteStage)
            completeStage()
          else if (canSlideFrame)
            slideFrame()
        }
      })
    }

    override def onPull(): Unit = pushUsingQueue()

    private def pushUsingQueue(next: Option[T] = None): Unit = {
      val maybeBuffer = bufferByInPort.get(currentHeadInPortIdx)
      if (maybeBuffer.forall(_.isEmpty) && next.nonEmpty) {
        push(out, next.get)
      } else if (maybeBuffer.exists(_.nonEmpty) && next.nonEmpty) {
        maybeBuffer.get.enqueue(next.get)
        push(out, maybeBuffer.get.dequeue())
      } else if (maybeBuffer.exists(_.nonEmpty) && next.isEmpty) {
        push(out, maybeBuffer.get.dequeue())
      } else {
        // Both empty
      }

      if (maybeBuffer.exists(_.isEmpty))
        bufferByInPort.remove(currentHeadInPortIdx)

      if (canCompleteStage)
        completeStage()
      else if (canSlideFrame)
        slideFrame()
    }

    override def preStart(): Unit = {
      if (breadth >= inputPorts) {
        in.foreach(pull)
        currentLastInPortIdx = overallLastInPortIdx
      } else {
        in.slice(0, breadth).foreach(pull)
        currentLastInPortIdx = breadth - 1
      }
    }

    private def canSlideFrame: Boolean =
      (!bufferByInPort.contains(currentHeadInPortIdx) || bufferByInPort(currentHeadInPortIdx).isEmpty) &&
        isClosed(in(currentHeadInPortIdx))

    private def canCompleteStage: Boolean =
      canSlideFrame && currentHeadInPortIdx == overallLastInPortIdx

    private def slideFrame(): Unit = {
      currentHeadInPortIdx += 1

      if (isAvailable(out))
        pushUsingQueue()

      if (currentLastInPortIdx != overallLastInPortIdx)
        currentLastInPortIdx += 1

      if (!hasBeenPulled(in(currentLastInPortIdx)))
        tryPull(in(currentLastInPortIdx))
    }
  }
}
