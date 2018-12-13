/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult}
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

  private val in = Inlet[MessageAction]("action")
  private val out = Outlet[Future[SqsAckResult]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      private var inIsClosed = false

      private var completionState: Option[Try[Unit]] = None

      private var failureCallback: AsyncCallback[Exception] = _
      private var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityRequest] = _
      private var deleteCallback: AsyncCallback[DeleteMessageRequest] = _

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
          log.debug("Set visibility timeout for message {} to {}",
                    request.getReceiptHandle,
                    request.getVisibilityTimeout)
          inFlight -= 1
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
        deleteCallback = getAsyncCallback[DeleteMessageRequest] { request =>
          log.debug("Deleted message {}", request.getReceiptHandle)
          inFlight -= 1
          if (inFlight == 0 && inIsClosed)
            checkForCompletion()
        }
      }

      override protected def logSource: Class[_] = classOf[SqsAckFlowStage]

      def checkForCompletion(): Unit =
        if (isClosed(in) && inFlight == 0) {
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
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            inIsClosed = true
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush(): Unit = {
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
                    responsePromise.success(new SqsAckResult(Some(result), action))
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
                    responsePromise.success(new SqsAckResult(Some(result), action))
                    changeVisibilityCallback.invoke(request)
                  }
                }
                sqsClient
                  .changeMessageVisibilityAsync(
                    new ChangeMessageVisibilityRequest(queueUrl, message.getReceiptHandle, change.visibilityTimeout),
                    handler
                  )

              case _: MessageAction.Ignore =>
                inFlight -= 1 // since there is no callback, we can directly decrease the inFlight counter
                responsePromise.success(new SqsAckResult(None, action))
            }
            push(out, responsePromise.future)
          }
        }
      )
    }
}
