/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import java.util

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{SqsBatchException, SqsPublishResult}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsBatchFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[SendMessageRequest], Future[List[SqsPublishResult]]]] {
  private val in = Inlet[Iterable[SendMessageRequest]]("messageBatch")
  private val out = Outlet[Future[List[SqsPublishResult]]]("batchResult")

  override def shape: FlowShape[Iterable[SendMessageRequest], Future[List[SqsPublishResult]]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var inFlight: Int = 0
      var inIsClosed = false
      var completionState: Option[Try[Done]] = None

      var failureCallback: AsyncCallback[SqsBatchException] = _
      var sendCallback: AsyncCallback[SendMessageBatchResult] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[SqsBatchException] { exception =>
          inFlight -= exception.batchSize
          failStage(exception)
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
        sendCallback = getAsyncCallback[SendMessageBatchResult] { result =>
          val batchSize = result.getSuccessful.size() + result.getFailed.size()
          inFlight -= batchSize
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
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
            completionState = Some(Success(Done))
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

            val responsePromise = Promise[List[SqsPublishResult]]

            val handler = new AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult] {
              override def onError(exception: Exception): Unit = {
                val batchException = new SqsBatchException(messages.size, exception)
                responsePromise.failure(batchException)
                failureCallback.invoke(batchException)
              }

              override def onSuccess(request: SendMessageBatchRequest, result: SendMessageBatchResult): Unit =
                if (!result.getFailed.isEmpty) {
                  val nrOfFailedMessages: Int = result.getFailed.size()
                  val batchException: SqsBatchException =
                    new SqsBatchException(
                      batchSize = messages.length,
                      cause = new Exception(
                        s"Some messages are failed to send. $nrOfFailedMessages of $nrOfMessages messages are failed"
                      )
                    )
                  responsePromise.failure(batchException)
                  failureCallback.invoke(batchException)
                } else {
                  val results = ListBuffer.empty[SqsPublishResult]

                  val successfulMessages = result.getSuccessful.iterator()
                  while (successfulMessages.hasNext) {
                    val successfulMessage: SendMessageBatchResultEntry = successfulMessages.next()
                    val messageBody: String = messages(successfulMessage.getId.toInt).getMessageBody

                    val sendMessageResult: SendMessageResult = new SendMessageResult()
                      .withMD5OfMessageAttributes(successfulMessage.getMD5OfMessageAttributes)
                      .withMD5OfMessageBody(successfulMessage.getMD5OfMessageBody)
                      .withMessageId(successfulMessage.getMessageId)
                      .withSequenceNumber(successfulMessage.getSequenceNumber)

                    results += SqsPublishResult(sendMessageResult, messageBody)
                  }

                  responsePromise.success(results.toList)
                  sendCallback.invoke(result)
                }
            }
            sqsClient.sendMessageBatchAsync(
              createMessageBatch(messages),
              handler
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
