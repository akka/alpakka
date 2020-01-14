/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.name
import akka.stream._
import akka.stream.impl.fusing.MapAsync
import akka.stream.impl.{BoundedBuffer, Buffer, FixedSizeBuffer}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * Internal API.
 *
 * Imported from `akka.stream.impl` for Akka 2.5/2.6 cross-compilation.
 */
@InternalApi private[impl] object BufferImpl {
  val FixedQueueSize = 128
  val FixedQueueMask = 127

  def apply[T](size: Int, materializer: Materializer): Buffer[T] =
    materializer match {
      case m: ActorMaterializer =>
        apply(size, m.settings.maxFixedBufferSize)
      case _ => apply(size, 1000000000)
    }

  private def apply[T](size: Int, max: Int): Buffer[T] =
    if (size < FixedQueueSize || size < max) FixedSizeBuffer(size)
    else new BoundedBuffer(size)
}

@InternalApi private[sqs] final case class BalancingMapAsync[In, Out](
    maxParallelism: Int,
    f: In => Future[Out],
    balancingF: (Out, Int) ⇒ Int
) extends GraphStage[FlowShape[In, Out]] {

  import MapAsync._

  private val in = Inlet[In]("BalancingMapAsync.in")
  private val out = Outlet[Out]("BalancingMapAsync.out")

  override def initialAttributes = name("BalancingMapAsync")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      var buffer: Buffer[Holder[Out]] = _
      var parallelism = maxParallelism

      private val futureCB = getAsyncCallback[Holder[Out]](
        holder =>
          holder.elem match {
            case Success(value) =>
              parallelism = balancingF(value, parallelism)
              pushNextIfPossible()
            case Failure(ex) =>
              holder.supervisionDirectiveFor(decider, ex) match {
                // fail fast as if supervision says so
                case Supervision.Stop => failStage(ex)
                case _ => pushNextIfPossible()
              }
          }
      )

      override def preStart(): Unit = buffer = BufferImpl(parallelism, materializer)

      override def onPull(): Unit = pushNextIfPossible()

      override def onPush(): Unit = {
        try {
          val future = f(grab(in))
          val holder = new Holder[Out](NotYetThere, futureCB)
          buffer.enqueue(holder)

          future.value match {
            case None => future.onComplete(holder)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
            case Some(v) =>
              // #20217 the future is already here, optimization: avoid scheduling it on the dispatcher and
              // run the logic directly on this thread
              holder.setElem(v)
              v match {
                // this optimization also requires us to stop the stage to fail fast if the decider says so:
                case Failure(ex) if holder.supervisionDirectiveFor(decider, ex) == Supervision.Stop => failStage(ex)
                case _ => pushNextIfPossible()
              }
          }

        } catch {
          // this logic must only be executed if f throws, not if the future is failed
          case NonFatal(ex) => if (decider(ex) == Supervision.Stop) failStage(ex)
        }

        pullIfNeeded()
      }

      override def onUpstreamFinish(): Unit = if (buffer.isEmpty) completeStage()

      @tailrec
      private def pushNextIfPossible(): Unit =
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pullIfNeeded()
        } else if (buffer.peek().elem eq NotYetThere) pullIfNeeded() // ahead of line blocking to keep order
        else if (isAvailable(out)) {
          val holder = buffer.dequeue()
          holder.elem match {
            case Success(elem) =>
              push(out, elem)
              pullIfNeeded()

            case Failure(NonFatal(ex)) =>
              holder.supervisionDirectiveFor(decider, ex) match {
                // this could happen if we are looping in pushNextIfPossible and end up on a failed future before the
                // onComplete callback has run
                case Supervision.Stop => failStage(ex)
                case _ =>
                  // try next element
                  pushNextIfPossible()
              }
          }
        }

      private def pullIfNeeded(): Unit =
        if (buffer.used < parallelism && !hasBeenPulled(in)) tryPull(in)

      setHandlers(in, out, this)
    }
}
