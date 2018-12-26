/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import java.util

import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResult}

import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsSourceStage(queueUrl: String, settings: SqsSourceSettings)(
    implicit sqsClient: AmazonSQSAsync
) extends GraphStage[SourceShape[Message]] {

  private val out: Outlet[Message] = Outlet("message")
  override val shape: SourceShape[Message] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val maxConcurrency = settings.maxBufferSize / settings.maxBatchSize
      private val buffer = new util.ArrayDeque[Message]()

      private val successCallback = getAsyncCallback[ReceiveMessageResult](handleSuccess)

      private val failureCallback = getAsyncCallback[Exception](handleFailure)

      private var maxCurrentConcurrency = maxConcurrency
      private var currentRequests = 0
      private var closeAfterDrain = false

      private def canReceiveNewMessages: Boolean = {
        val currentFreeRequests = (settings.maxBufferSize - buffer.size) / settings.maxBatchSize
        currentFreeRequests > currentRequests &&
        maxCurrentConcurrency > currentRequests &&
        !closeAfterDrain
      }

      private def shouldTerminateStage: Boolean =
        closeAfterDrain &&
        currentRequests == 0 &&
        buffer.isEmpty

      def receiveMessages(): Unit = {

        currentRequests = currentRequests + 1

        var request = new ReceiveMessageRequest(queueUrl)
          .withAttributeNames(settings.attributeNames.map(_.name).asJava)
          .withMessageAttributeNames(settings.messageAttributeNames.map(_.name).asJava)
          .withMaxNumberOfMessages(settings.maxBatchSize)
          .withWaitTimeSeconds(settings.waitTimeSeconds)

        request = setVisibilityTimeoutIfExists(request)

        sqsClient.receiveMessageAsync(
          request,
          new AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult] {
            override def onError(e: Exception): Unit =
              failureCallback.invoke(e)

            override def onSuccess(request: ReceiveMessageRequest, result: ReceiveMessageResult): Unit =
              successCallback.invoke(result)
          }
        )
      }

      def handleFailure(ex: Exception): Unit =
        failStage(ex)

      def handleSuccess(result: ReceiveMessageResult): Unit = {
        currentRequests = currentRequests - 1
        maxCurrentConcurrency = if (result.getMessages.isEmpty) 1 else maxConcurrency

        val receivedMessages = result.getMessages.asScala
        receivedMessages.foreach(buffer.offer)

        if (receivedMessages.isEmpty && settings.closeOnEmptyReceive) {
          closeAfterDrain = true
        }

        if (!buffer.isEmpty && isAvailable(out)) {
          push(out, buffer.poll())
        }

        receiveMoreOrComplete()
      }

      private def setVisibilityTimeoutIfExists(request: ReceiveMessageRequest): ReceiveMessageRequest =
        settings.visibilityTimeout
          .map(_.toSeconds.toInt)
          .map(request.withVisibilityTimeout(_))
          .getOrElse(request)

      private def receiveMoreOrComplete(): Unit =
        if (canReceiveNewMessages) {
          receiveMessages()
        } else if (shouldTerminateStage) {
          completeStage()
        }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            if (!buffer.isEmpty) {
              push(out, buffer.poll())
              receiveMoreOrComplete()
            } else {
              receiveMessages()
            }
        }
      )
    }
}
