/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awsses

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceAsyncClient
import com.amazonaws.services.simpleemail.model.{SendEmailRequest, SendEmailResult}

import scala.util.control.NonFatal

final class AwsSesFlowStage(awsSesClient: AmazonSimpleEmailServiceAsyncClient)(parallelism: Int)
    extends GraphStage[FlowShape[SendEmailRequest, SendEmailResult]] {

  val in: Inlet[SendEmailRequest] = Inlet[SendEmailRequest]("AwsSes.in")
  val out: Outlet[SendEmailResult] = Outlet[SendEmailResult]("AwsSes.out")

  @throws(classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var inFlight = 0

      private def invokeComplete(result: SendEmailResult): Unit = {
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

      private val asyncHandler = new AsyncHandler[SendEmailRequest, SendEmailResult] {
        override def onError(exception: Exception): Unit =
          exception match {
            case NonFatal(ex) =>
              failureHandler.invoke(ex)
          }

        override def onSuccess(request: SendEmailRequest, result: SendEmailResult): Unit =
          successHandler.invoke(result)
      }

      override def onPush(): Unit = {
        inFlight += 1
        awsSesClient.sendEmailAsync(grab(in), asyncHandler)
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

  override val shape: FlowShape[SendEmailRequest, SendEmailResult] = FlowShape.of(in, out)

}
