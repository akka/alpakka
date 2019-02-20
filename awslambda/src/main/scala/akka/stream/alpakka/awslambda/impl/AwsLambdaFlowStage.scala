/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.awslambda.impl

import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.lambda.AWSLambdaAsync
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}

import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi private[awslambda] final class AwsLambdaFlowStage(awsLambdaClient: AWSLambdaAsync)(parallelism: Int)
    extends GraphStage[FlowShape[InvokeRequest, InvokeResult]] {

  val in = Inlet[InvokeRequest]("AwsLambda.in")
  val out = Outlet[InvokeResult]("AwsLambda.out")

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var inFlight = 0

      private def invokeComplete(result: InvokeResult): Unit = {
        inFlight -= 1
        if (isAvailable(out)) {
          if (!hasBeenPulled(in)) tryPull(in)
          push(out, result)
        }
      }

      private def invokeFailed(ex: Throwable): Unit =
        failStage(ex)

      private val successHandler = getAsyncCallback(invokeComplete)
      private val failureHandler = getAsyncCallback(invokeFailed)

      private val asyncHandler = new AsyncHandler[InvokeRequest, InvokeResult] {
        override def onError(exception: Exception): Unit =
          exception match {
            case NonFatal(ex) =>
              failureHandler.invoke(ex)
          }

        override def onSuccess(request: InvokeRequest, result: InvokeResult): Unit =
          successHandler.invoke(result)
      }

      override def onPush(): Unit = {
        inFlight += 1
        awsLambdaClient.invokeAsync(grab(in), asyncHandler)
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

  override val shape: FlowShape[InvokeRequest, InvokeResult] = FlowShape.of(in, out)

}
