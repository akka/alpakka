/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util

import akka.stream.alpakka.sqs.scaladsl.Result
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private[sqs] final class SqsBatchFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[SendMessageRequest], Future[List[Result]]]] {
  private val in = Inlet[Iterable[SendMessageRequest]]("messageBatch")
  private val out = Outlet[Future[List[Result]]]("batchResult")

  override def shape: FlowShape[Iterable[SendMessageRequest], Future[List[Result]]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var inFlight: Int = 0
      var inIsClosed = false
      var completionState: Option[Try[Unit]] = None

      var failureCallback: AsyncCallback[BatchException] = _
      var sendCallback: AsyncCallback[SendMessageBatchResult] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[BatchException](handleFailure)
        sendCallback = getAsyncCallback[SendMessageBatchResult](handleSend)
      }

      private def handleFailure(exception: BatchException): Unit = {
        inFlight -= exception.batchSize
        failStage(exception)
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }

      private def handleSend(result: SendMessageBatchResult): Unit = {
        val batchSize = result.getSuccessful.size() + result.getFailed.size()
        inFlight -= batchSize
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }

      def checkForCompletion() =
        if (isClosed(in) && inFlight <= 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      setHandler(out, new OutHandler {
        override def onPull() =
          tryPull(in)
      })

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish() = {
            inIsClosed = true
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable) = {
            inIsClosed = true
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush() = {
            val messages: Array[SendMessageRequest] = grab(in).toArray
            val nrOfMessages = messages.length

            inFlight += nrOfMessages

            val responsePromise = Promise[List[Result]]

            val batch: SendMessageBatchRequest = createMessageBatch(messages)
            sqsClient.sendMessageBatchAsync(
              batch,
              new AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult] {
                override def onError(exception: Exception): Unit = {
                  val batchException = BatchException(messages.size, exception)
                  responsePromise.failure(batchException)
                  failureCallback.invoke(batchException)
                }

                override def onSuccess(request: SendMessageBatchRequest, result: SendMessageBatchResult): Unit =
                  if (!result.getFailed.isEmpty) {
                    val nrOfFailedMessages: Int = result.getFailed.size()
                    val batchException: BatchException =
                      BatchException(
                        batchSize = messages.length,
                        cause = new Exception(
                          s"Some messages are failed to send. $nrOfFailedMessages of $nrOfMessages messages are failed"
                        )
                      )
                    responsePromise.failure(batchException)
                    failureCallback.invoke(batchException)
                  } else {
                    val results = ListBuffer.empty[Result]

                    val successfulMessages = result.getSuccessful.iterator()
                    while (successfulMessages.hasNext) {
                      val successfulMessage: SendMessageBatchResultEntry = successfulMessages.next()
                      val messageBody: String = messages(successfulMessage.getId.toInt).getMessageBody

                      val sendMessageResult: SendMessageResult = new SendMessageResult()
                        .withMD5OfMessageAttributes(successfulMessage.getMD5OfMessageAttributes)
                        .withMD5OfMessageBody(successfulMessage.getMD5OfMessageBody)
                        .withMessageId(successfulMessage.getMessageId)
                        .withSequenceNumber(successfulMessage.getSequenceNumber)

                      results += Result(sendMessageResult, messageBody)
                    }

                    responsePromise.success(results.toList)
                    sendCallback.invoke(result)
                  }
              }
            )
            push(out, responsePromise.future)
          }

          private def createMessageBatch(messages: Array[SendMessageRequest]): SendMessageBatchRequest = {
            val messageRequestEntries: util.List[SendMessageBatchRequestEntry] =
              new util.ArrayList[SendMessageBatchRequestEntry]()
            var id = 0
            messages.foreach { message =>
              val entry = new SendMessageBatchRequestEntry(id.toString, message.getMessageBody)
              entry.setMessageAttributes(message.getMessageAttributes)
              messageRequestEntries.add(entry)
              id += 1
            }

            val batchRequest = new SendMessageBatchRequest(queueUrl, messageRequestEntries)
            batchRequest
          }
        }
      )
    }
}
