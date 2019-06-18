/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.name
import akka.stream.impl.{ReactiveStreamsCompliance, Buffer => BufferImpl}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class BalancingMapAsyncUnordered[In, Out](
    initialParallelism: Int,
    f: In ⇒ Future[Out],
    balancingF: Out ⇒ Int
) extends GraphStage[FlowShape[In, Out]] {

  private val in = Inlet[In]("BalancingMapAsyncUnordered.in")
  private val out = Outlet[Out]("BalancingMapAsyncUnordered.out")

  override def initialAttributes = name("BalancingMapAsyncUnordered")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def toString = s"BalancingMapAsyncUnordered.Logic(inFlight=$inFlight, buffer=$buffer)"

      val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var inFlight = 0
      private var buffer: BufferImpl[Out] = _
      private var parallelism = initialParallelism

      private[this] def todo = inFlight + buffer.used

      override def preStart(): Unit = buffer = BufferImpl(initialParallelism, materializer)

      def futureCompleted(result: Try[Out]): Unit = {
        inFlight -= 1
        result match {
          case Success(elem) if elem != null ⇒
            parallelism = balancingF(elem)
            if (isAvailable(out)) {
              if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
              push(out, elem)
            } else buffer.enqueue(elem)
          case other ⇒
            val ex = other match {
              case Failure(t) ⇒ t
              case Success(s) if s == null ⇒ ReactiveStreamsCompliance.elementMustNotBeNullException
            }
            if (decider(ex) == Supervision.Stop) failStage(ex)
            else if (isClosed(in) && todo == 0) completeStage()
            else if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
        }
      }

      private val futureCB = getAsyncCallback(futureCompleted)
      private val invokeFutureCB: Try[Out] ⇒ Unit = futureCB.invoke

      override def onPush(): Unit = {
        try {
          val future = f(grab(in))
          inFlight += 1
          future.value match {
            case None ⇒ future.onComplete(invokeFutureCB)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
            case Some(v) ⇒ futureCompleted(v)
          }
        } catch {
          case NonFatal(ex) ⇒ if (decider(ex) == Supervision.Stop) failStage(ex)
        }
        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit =
        if (todo == 0) completeStage()

      override def onPull(): Unit = {
        if (!buffer.isEmpty) push(out, buffer.dequeue())
        else if (isClosed(in) && todo == 0) completeStage()

        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)
    }
}
