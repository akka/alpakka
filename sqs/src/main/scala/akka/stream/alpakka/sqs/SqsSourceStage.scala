/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.{ Message, ReceiveMessageRequest, ReceiveMessageResult }

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object SqsSourceSettings {
  val Defaults = SqsSourceSettings(20.seconds, 100, 10)
}

final case class SqsSourceSettings(longPollingDuration: FiniteDuration, maxBufferSize: Int, maxBatchSize: Int)

final class SqsSourceStage(queueUrl: String, settings: SqsSourceSettings, sqsClient: AmazonSQSAsyncClient) extends GraphStage[SourceShape[Message]] {

  val out: Outlet[Message] = Outlet("SqsSource.out")
  override val shape: SourceShape[Message] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      private val buffer = mutable.Queue[Message]()

      private val successCallback = getAsyncCallback[ReceiveMessageResult](handleSuccess)
      private val failureCallback = getAsyncCallback[Exception](handleFailure)

      def receiveMessages(): Unit = {

        val request = new ReceiveMessageRequest(queueUrl)
          .withMaxNumberOfMessages(settings.maxBatchSize)
          .withWaitTimeSeconds(settings.longPollingDuration.toSeconds.toInt)

        sqsClient.receiveMessageAsync(request, new AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult] {
          override def onError(e: Exception): Unit = {
            failureCallback.invoke(e)
          }

          override def onSuccess(request: ReceiveMessageRequest, result: ReceiveMessageResult): Unit =
            successCallback.invoke(result)
        })
      }

      def handleFailure(ex: Exception): Unit = {
        failStage(ex)
      }

      def handleSuccess(result: ReceiveMessageResult): Unit = {

        buffer.enqueue(result.getMessages.asScala.reverse: _*)

        if (buffer.nonEmpty && isAvailable(out)) {
          push(out, buffer.dequeue())
        }

        if (buffer.size < settings.maxBufferSize - settings.maxBatchSize) {
          receiveMessages()
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (buffer.nonEmpty) {
            if (buffer.size == settings.maxBufferSize - settings.maxBatchSize) {
              receiveMessages()
            }
            push(out, buffer.dequeue())
          } else {
            receiveMessages()
          }
        }
      })

    }
  }
}
