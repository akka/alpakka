/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws

import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import java.util.concurrent.CompletableFuture

import scala.util.{Failure, Success}
import scala.compat.java8.FutureConverters._
@InternalApi private[aws] abstract class AsyncHandlerFlowStage[In, Out](parallelism: Int)
    extends GraphStage[FlowShape[In, Out]] {

  val in: Inlet[In] = Inlet[In](s"${this.getClass.getSimpleName}.in")
  val out: Outlet[Out] = Outlet[Out](s"${this.getClass.getSimpleName}.out")

  /**
   * Client async invocation method
   * @param request
   * @param handler
   * @return
   */
  def clientInvoke(request: In): CompletableFuture[Out]
  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var inFlight = 0

      private def invokeComplete(result: Out): Unit = {
        inFlight -= 1
        if (isAvailable(out)) {
          if (!hasBeenPulled(in)) tryPull(in)
          push(out, result)
        }
      }

      private def invokeFailed(ex: Throwable): Unit =
        failStage(ex)

      override def onPush(): Unit = {
        inFlight += 1
        implicit val ec = scala.concurrent.ExecutionContext.global
        clientInvoke(grab(in)).toScala.onComplete {
          case Success(response) => invokeComplete(response)
          case Failure(t) => invokeFailed(t)
        }
        if (inFlight < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit =
        if (inFlight == 0) completeStage()

      override def onPull(): Unit = {
        if (isClosed(in) && inFlight == 0) completeStage()
        if (inFlight < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)

    }

  override val shape: FlowShape[In, Out] = FlowShape.of(in, out)
}
