/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns

import akka.stream._
import akka.stream.stage._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}

private[akka] final class SnsPublishFlowStage(topicArn: String, snsClient: AmazonSNSAsync)
    extends GraphStage[FlowShape[String, PublishResult]] {

  private val in = Inlet[String]("SnsPublishFlow.in")
  private val out = Outlet[PublishResult]("SnsPublishFlow.out")

  override def shape: FlowShape[String, PublishResult] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      private var inFlight = 0
      private val failureCallback = getAsyncCallback[Throwable](handleFailure)
      private val successCallback = getAsyncCallback[PublishResult](handleSuccess)

      private def handleFailure(ex: Throwable): Unit =
        failStage(ex)

      private def handleSuccess(result: PublishResult): Unit = {
        log.debug("Published SNS message: {}", result.getMessageId)
        inFlight -= 1
        if (isAvailable(out)) {
          if (!hasBeenPulled(in)) tryPull(in)
          push(out, result)
        }
      }

      private val asyncHandler = new AsyncHandler[PublishRequest, PublishResult] {
        override def onError(exception: Exception): Unit =
          failureCallback.invoke(exception)
        override def onSuccess(request: PublishRequest, result: PublishResult): Unit =
          successCallback.invoke(result)
      }

      override def onPush(): Unit = {
        inFlight += 1
        val request = new PublishRequest().withTopicArn(topicArn).withMessage(grab(in))
        snsClient.publishAsync(request, asyncHandler)
      }

      override def onPull(): Unit = {
        if (isClosed(in) && inFlight == 0) completeStage()
        if (!hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit =
        if (inFlight == 0) completeStage()

      setHandlers(in, out, this)
    }
}
