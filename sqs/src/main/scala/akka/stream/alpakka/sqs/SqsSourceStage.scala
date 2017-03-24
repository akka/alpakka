/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import java.util

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResult}

import scala.collection.JavaConverters._

object SqsSourceSettings {
  val Defaults = SqsSourceSettings(20, 100, 10)

  def create(waitTimeSeconds: Int, maxBufferSize: Int, maxBatchSize: Int): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds, maxBufferSize, maxBatchSize)

}

//#SqsSourceSettings
final case class SqsSourceSettings(
    waitTimeSeconds: Int,
    maxBufferSize: Int,
    maxBatchSize: Int
) {
  require(maxBatchSize <= maxBufferSize, "maxBatchSize must be lower or equal than maxBufferSize")
  // SQS requirements
  require(0 <= waitTimeSeconds && waitTimeSeconds <= 20,
    s"Invalid value ($waitTimeSeconds) for waitTimeSeconds. Requirement: 0 <= waitTimeSeconds <= 20 ")
  require(1 <= maxBatchSize && maxBatchSize <= 10,
    s"Invalid value ($maxBatchSize) for maxBatchSize. Requirement: 1 <= maxBatchSize <= 10 ")
}
//#SqsSourceSettings

final class SqsSourceStage(queueUrl: String, settings: SqsSourceSettings)(implicit sqsClient: AmazonSQSAsync)
    extends GraphStage[SourceShape[Message]] {

  val out: Outlet[Message] = Outlet("SqsSource.out")
  override val shape: SourceShape[Message] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val maxConcurrency = settings.maxBufferSize / settings.maxBatchSize
      private val buffer = new util.ArrayDeque[Message]()

      private val successCallback = getAsyncCallback[ReceiveMessageResult](handleSuccess)

      private val failureCallback = getAsyncCallback[Exception](handleFailure)

      private var maxCurrentConcurrency = maxConcurrency
      private var currentRequests = 0

      private def canReceiveNewMessages = {
        val currentFreeRequests = (settings.maxBufferSize - buffer.size) / settings.maxBatchSize
        currentFreeRequests > currentRequests && maxCurrentConcurrency > currentRequests
      }

      def receiveMessages(): Unit = {

        currentRequests = currentRequests + 1

        val request = new ReceiveMessageRequest(queueUrl)
          .withMaxNumberOfMessages(settings.maxBatchSize)
          .withWaitTimeSeconds(settings.waitTimeSeconds)

        sqsClient.receiveMessageAsync(request,
          new AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult] {
          override def onError(e: Exception): Unit =
            failureCallback.invoke(e)

          override def onSuccess(request: ReceiveMessageRequest, result: ReceiveMessageResult): Unit =
            successCallback.invoke(result)
        })
      }

      def handleFailure(ex: Exception): Unit =
        failStage(ex)

      def handleSuccess(result: ReceiveMessageResult): Unit = {

        currentRequests = currentRequests - 1
        maxCurrentConcurrency = if (result.getMessages.isEmpty) 1 else maxConcurrency

        result.getMessages.asScala.reverse.foreach(buffer.offer)

        if (!buffer.isEmpty && isAvailable(out)) {
          push(out, buffer.poll())
        }

        if (canReceiveNewMessages) {
          receiveMessages()
        }
      }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.poll())
            if (canReceiveNewMessages) {
              receiveMessages()
            }
          } else {
            receiveMessages()
          }
      })
    }
}
