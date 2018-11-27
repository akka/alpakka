/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{FifoMessageIdentifiers, SqsBatchException, SqsPublishResult}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsBatchFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[SendMessageRequest], Future[List[SqsPublishResult]]]] {

  private val in = Inlet[Iterable[SendMessageRequest]]("requests")
  private val out = Outlet[Future[List[SqsPublishResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var inFlight: Int = 0
      private var inIsClosed = false
      private var completionState: Option[Try[Done]] = None

      private var failureCallback: AsyncCallback[SqsBatchException] = _
      private var sendCallback: AsyncCallback[SendMessageBatchResult] = _

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

      private def checkForCompletion(): Unit =
        if (isClosed(in) && inFlight <= 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          tryPull(in)
      })

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = {
            inIsClosed = true
            completionState = Some(Success(Done))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            inIsClosed = true
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush(): Unit = {
            val messages = grab(in).toList
            val nrOfMessages = messages.length

            inFlight += nrOfMessages

            val responsePromise = Promise[List[SqsPublishResult]]

            val handler = new AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult] {
              override def onError(exception: Exception): Unit = {
                val batchException = new SqsBatchException(nrOfMessages, exception)
                responsePromise.failure(batchException)
                failureCallback.invoke(batchException)
              }

              override def onSuccess(request: SendMessageBatchRequest, result: SendMessageBatchResult): Unit =
                if (!result.getFailed.isEmpty) {
                  val nrOfFailedMessages = result.getFailed.size()
                  val batchException = new SqsBatchException(
                    batchSize = nrOfMessages,
                    cause = new Exception(
                      s"Some messages are failed to send. $nrOfFailedMessages of $nrOfMessages messages are failed"
                    )
                  )
                  responsePromise.failure(batchException)
                  failureCallback.invoke(batchException)
                } else {
                  val requestEntries = request.getEntries.asScala.map(e => e.getId -> e).toMap
                  val messages = result.getSuccessful.asScala.map {
                    resp =>
                      val req = requestEntries(resp.getId)

                      val message = new Message()
                        .withMessageId(resp.getMessageId)
                        .withBody(req.getMessageBody)
                        .withMD5OfBody(resp.getMD5OfMessageBody)
                        .withMessageAttributes(req.getMessageAttributes)
                        .withMD5OfMessageAttributes(resp.getMD5OfMessageAttributes)

                      val fifoIdentifiers = Option(resp.getSequenceNumber).map { sequenceNumber =>
                        val messageGroupId = req.getMessageGroupId
                        val messageDeduplicationId = Option(req.getMessageDeduplicationId)
                        FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId)
                      }

                      SqsPublishResult(result, message, fifoIdentifiers)
                  }
                  responsePromise.success(messages.toList)
                  sendCallback.invoke(result)
                }
            }
            sqsClient.sendMessageBatchAsync(
              createBatchRequest(messages),
              handler
            )
            push(out, responsePromise.future)
          }

          private def createBatchRequest(requests: Seq[SendMessageRequest]): SendMessageBatchRequest = {
            val entries = requests.zipWithIndex.map {
              case (r, i) =>
                new SendMessageBatchRequestEntry()
                  .withId(i.toString)
                  .withMessageBody(r.getMessageBody)
                  .withMessageAttributes(r.getMessageAttributes)
            }

            new SendMessageBatchRequest()
              .withQueueUrl(queueUrl)
              .withEntries(entries.asJava)
          }
        }
      )
    }
}
