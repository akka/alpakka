/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import java.util
import java.util.concurrent.Executors

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResult}

import scala.collection.JavaConverters._

object SqsSourceSettings {
  val Defaults = SqsSourceSettings(20, 100, 10, None)

  def create(waitTimeSeconds: Int,
             maxBufferSize: Int,
             maxBatchSize: Int,
             credentials: AWSCredentials): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds, maxBufferSize, maxBatchSize, Some(credentials))

  private def create(waitTimeSeconds: Int, maxBufferSize: Int, maxBatchSize: Int): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds, maxBufferSize, maxBatchSize, None)

}

//#SqsSourceSettings
final case class SqsSourceSettings(
    waitTimeSeconds: Int,
    maxBufferSize: Int,
    maxBatchSize: Int,
    credentials: Option[AWSCredentials]
) {
  require(maxBatchSize <= maxBufferSize)
  // SQS requirements
  require(waitTimeSeconds >= 0 && waitTimeSeconds <= 20)
  require(maxBatchSize >= 1 && maxBatchSize <= 10)
}
//#SqsSourceSettings

final class SqsSourceStage(queueUrl: String, settings: SqsSourceSettings) extends GraphStage[SourceShape[Message]] {

  val out: Outlet[Message] = Outlet("SqsSource.out")
  override val shape: SourceShape[Message] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val credentials =
        settings.credentials.getOrElse(DefaultAWSCredentialsProviderChain.getInstance().getCredentials)

      private val maxConcurrency = settings.maxBufferSize / settings.maxBatchSize
      private val threadPool = Executors.newFixedThreadPool(maxConcurrency)
      private val sqsClient = new AmazonSQSAsyncClient(credentials, threadPool)
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
        maxCurrentConcurrency = if (result.getMessages.size() == 0) 1 else maxConcurrency

        result.getMessages.asScala.reverse.foreach(buffer.addFirst)

        if (!buffer.isEmpty && isAvailable(out)) {
          push(out, buffer.removeLast())
        }

        if (canReceiveNewMessages) {
          receiveMessages()
        }
      }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.removeLast())
            if (canReceiveNewMessages) {
              receiveMessages()
            }
          } else {
            receiveMessages()
          }
      })

    }
}
