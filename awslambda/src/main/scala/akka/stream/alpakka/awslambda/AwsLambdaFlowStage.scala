/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awslambda

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.lambda.AWSLambdaAsyncClient
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}

import scala.util.control.NonFatal

class AwsLambdaFlowStage(
                          awsLambdaClient: AWSLambdaAsyncClient
                        )(parallelism: Int)
  extends GraphStage[FlowShape[InvokeRequest, InvokeResult]] {

  val in = Inlet[InvokeRequest]("AwsLambda.in")
  val out = Outlet[InvokeResult]("AwsLambda.out")

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var inFlight = 0

      def invokeComplete(result: InvokeResult): Unit = {
        inFlight -= 1
        if (isAvailable(out)) {
          if (!hasBeenPulled(in)) tryPull(in)
          push(out, result)
        }
      }

      private val asyncCallback = getAsyncCallback(invokeComplete)

      private val asyncHandler = new AsyncHandler[InvokeRequest, InvokeResult] {
        override def onError(exception: Exception): Unit =
          exception match {
            case NonFatal(ex) => failStage(ex)
          }

        override def onSuccess(request: InvokeRequest, result: InvokeResult): Unit =
          asyncCallback.invoke(result)
      }

      override def onPush(): Unit = {
        inFlight += 1
        awsLambdaClient.invokeAsync(grab(in), asyncHandler)
        if (inFlight < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit =
        if (inFlight == 0) completeStage()

      override def onPull(): Unit = {
        if (isClosed(in)) completeStage()
        if (inFlight < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)

    }

  override def shape: FlowShape[InvokeRequest, InvokeResult] = FlowShape.of(in, out)

}
