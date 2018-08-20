/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{SqsAckResult, MessageAction}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsAckFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[MessageAction, Future[SqsAckResult]]] {

  private val in = Inlet[MessageAction]("messages")
  private val out = Outlet[Future[SqsAckResult]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      private var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      var failureCallback: AsyncCallback[Exception] = _
      var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityRequest] = _
      var deleteCallback: AsyncCallback[DeleteMessageRequest] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[Exception] { exception =>
          log.error(exception, "Client failure: {}", exception)
          inFlight -= 1
          failStage(exception)
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
        changeVisibilityCallback = getAsyncCallback[ChangeMessageVisibilityRequest] { request =>
          log.debug(s"Set visibility timeout for message {} to {}",
                    request.getReceiptHandle,
                    request.getVisibilityTimeout)
          inFlight -= 1
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
        deleteCallback = getAsyncCallback[DeleteMessageRequest] { request =>
          log.debug(s"Deleted message {}", request.getReceiptHandle)
          inFlight -= 1
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
      }

      override protected def logSource: Class[_] = classOf[SqsAckFlowStage]

      def checkForCompletion() =
        if (isClosed(in) && inFlight == 0) {
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
            inFlight += 1
            val action = grab(in)
            val message = action.message
            val responsePromise = Promise[SqsAckResult]
            action match {
              case _: MessageAction.Delete =>
                val handler = new AsyncHandler[DeleteMessageRequest, DeleteMessageResult] {

                  override def onError(exception: Exception): Unit = {
                    responsePromise.failure(exception)
                    failureCallback.invoke(exception)
                  }

                  override def onSuccess(request: DeleteMessageRequest, result: DeleteMessageResult): Unit = {
                    responsePromise.success(SqsAckResult(Some(result), message.getBody))
                    deleteCallback.invoke(request)
                  }
                }
                sqsClient.deleteMessageAsync(
                  new DeleteMessageRequest(queueUrl, message.getReceiptHandle),
                  handler
                )

              case change: MessageAction.ChangeMessageVisibility =>
                val handler = new AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult] {

                  override def onError(exception: Exception): Unit = {
                    responsePromise.failure(exception)
                    failureCallback.invoke(exception)
                  }

                  override def onSuccess(request: ChangeMessageVisibilityRequest,
                                         result: ChangeMessageVisibilityResult): Unit = {
                    responsePromise.success(SqsAckResult(Some(result), message.getBody))
                    changeVisibilityCallback.invoke(request)
                  }
                }
                sqsClient
                  .changeMessageVisibilityAsync(
                    new ChangeMessageVisibilityRequest(queueUrl, message.getReceiptHandle, change.visibilityTimeout),
                    handler
                  )

              case _: MessageAction.Ignore =>
                responsePromise.success(SqsAckResult(None, message.getBody))
            }
            push(out, responsePromise.future)
          }
        }
      )
    }
}
