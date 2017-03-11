/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns

import akka.stream._
import akka.stream.stage._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}

final class SnsPublishFlowStage(topicArn: String, snsClient: AmazonSNSAsync)
    extends GraphStage[FlowShape[String, PublishResult]] {

  private val in = Inlet[String]("SnsPublishFlow.in")
  private val out = Outlet[PublishResult]("SnsPublishFlow.out")

  override def shape: FlowShape[String, PublishResult] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      private val failureCallback = getAsyncCallback[Throwable](handleFailure)
      private val successCallback = getAsyncCallback[PublishResult](handleSuccess)

      private def handleFailure(ex: Throwable): Unit =
        failStage(ex)

      private def handleSuccess(result: PublishResult): Unit = {
        log.debug("Published SNS message: {}", result.getMessageId)

        if (isAvailable(out)) {
          if (!hasBeenPulled(in)) tryPull(in)
          push(out, result)
        }
      }

      override def onPush(): Unit = {
        val request = new PublishRequest().withTopicArn(topicArn).withMessage(grab(in))

        snsClient.publishAsync(request,
          new AsyncHandler[PublishRequest, PublishResult] {
          override def onError(exception: Exception): Unit =
            failureCallback.invoke(exception)

          override def onSuccess(request: PublishRequest, result: PublishResult): Unit =
            successCallback.invoke(result)
        })
      }

      override def onPull(): Unit = {
        if (isClosed(in)) completeStage()
        if (!hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)
    }
  }
}
