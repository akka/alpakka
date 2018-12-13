/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{FifoMessageIdentifiers, SqsPublishResult}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{Message, SendMessageRequest, SendMessageResult}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsFlowStage(sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[SendMessageRequest, Future[SqsPublishResult]]] {

  private val in = Inlet[SendMessageRequest]("request")
  private val out = Outlet[Future[SqsPublishResult]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      private var inIsClosed = false
      private var completionState: Option[Try[Done]] = None

      override protected def logSource: Class[_] = classOf[SqsFlowStage]

      var failureCallback: AsyncCallback[Exception] = _
      var sendCallback: AsyncCallback[SendMessageResult] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[Exception] { exception =>
          log.error(exception, "Client failure: {}", exception.getMessage)
          inFlight -= 1
          failStage(exception)
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
        sendCallback = getAsyncCallback[SendMessageResult] { result =>
          log.debug(s"Sent message {}", result.getMessageId)
          inFlight -= 1
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
      }

      def checkForCompletion(): Unit =
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
            inFlight += 1
            val request = grab(in)

            val responsePromise = Promise[SqsPublishResult]

            val handler = new AsyncHandler[SendMessageRequest, SendMessageResult] {

              override def onError(exception: Exception): Unit = {
                responsePromise.failure(exception)
                failureCallback.invoke(exception)
              }

              override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit = {
                val message = new Message()
                  .withMessageId(result.getMessageId)
                  .withBody(request.getMessageBody)
                  .withMD5OfBody(result.getMD5OfMessageBody)
                  .withMessageAttributes(request.getMessageAttributes)
                  .withMD5OfMessageAttributes(result.getMD5OfMessageAttributes)

                val fifoIdentifiers = Option(result.getSequenceNumber).map { sequenceNumber =>
                  val messageGroupId = request.getMessageGroupId
                  val messageDeduplicationId = Option(request.getMessageDeduplicationId)
                  new FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId)
                }

                responsePromise.success(new SqsPublishResult(result, message, fifoIdentifiers))
                sendCallback.invoke(result)
              }
            }
            sqsClient.sendMessageAsync(
              request,
              handler
            )
            push(out, responsePromise.future)
          }
        }
      )
    }
}
