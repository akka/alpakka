/*
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import java.util

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResult}

import scala.collection.JavaConverters._

object SqsSourceSettings {
  val Defaults = SqsSourceSettings(20, 100, 10)
}

//#SqsSourceSettings
final case class SqsSourceSettings(waitTimeSeconds: Int, maxBufferSize: Int, maxBatchSize: Int) {
  require(maxBatchSize <= maxBufferSize)
  // SQS requirements
  require(waitTimeSeconds >= 0 && waitTimeSeconds <= 20)
  require(maxBatchSize >= 1 && maxBatchSize <= 10)
}
//#SqsSourceSettings

final class SqsSourceStage(queueUrl: String, settings: SqsSourceSettings, sqsClient: AmazonSQSAsyncClient)
    extends GraphStage[SourceShape[Message]] {

  val out: Outlet[Message] = Outlet("SqsSource.out")
  override val shape: SourceShape[Message] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val buffer = new util.ArrayDeque[Message]()

      private val successCallback = getAsyncCallback[ReceiveMessageResult](handleSuccess)
      private val failureCallback = getAsyncCallback[Exception](handleFailure)

      def receiveMessages(): Unit = {

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

        result.getMessages.asScala.reverse.foreach(buffer.addFirst)

        if (!buffer.isEmpty && isAvailable(out)) {
          push(out, buffer.removeLast())
        }

        if (buffer.size < settings.maxBufferSize - settings.maxBatchSize) {
          receiveMessages()
        }
      }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit =
          if (!buffer.isEmpty) {
            if (buffer.size == settings.maxBufferSize - settings.maxBatchSize) {
              receiveMessages()
            }
            push(out, buffer.removeLast())
          } else {
            receiveMessages()
          }
      })

    }
}
