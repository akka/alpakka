/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.scaladsl.AckResult
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private[sqs] final class SqsAckFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[MessageActionPair, Future[AckResult]]] {

  private val in = Inlet[MessageActionPair]("messages")
  private val out = Outlet[Future[AckResult]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      private var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      private def handleFailure(exception: Exception): Unit = {
        log.error(exception, "Client failure: {}", exception.getMessage)
        inFlight -= 1
        failStage(exception)
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }

      private def handleChangeVisibility(request: ChangeMessageVisibilityRequest): Unit = {
        log.debug(s"Set visibility timeout for message {} to {}",
                  request.getReceiptHandle,
                  request.getVisibilityTimeout)
        inFlight -= 1
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }
      private def handleDelete(request: DeleteMessageRequest): Unit = {
        log.debug(s"Deleted message {}", request.getReceiptHandle)
        inFlight -= 1
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }

      var failureCallback: AsyncCallback[Exception] = _
      var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityRequest] = _
      var deleteCallback: AsyncCallback[DeleteMessageRequest] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[Exception](handleFailure)
        changeVisibilityCallback = getAsyncCallback[ChangeMessageVisibilityRequest](handleChangeVisibility)
        deleteCallback = getAsyncCallback[DeleteMessageRequest](handleDelete)
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
            val (message, action) = grab(in)
            val responsePromise = Promise[AckResult]
            action match {
              case MessageAction.Delete =>
                sqsClient.deleteMessageAsync(
                  new DeleteMessageRequest(queueUrl, message.getReceiptHandle),
                  new AsyncHandler[DeleteMessageRequest, DeleteMessageResult] {

                    override def onError(exception: Exception): Unit = {
                      responsePromise.failure(exception)
                      failureCallback.invoke(exception)
                    }

                    override def onSuccess(request: DeleteMessageRequest, result: DeleteMessageResult): Unit = {
                      responsePromise.success(AckResult(Some(result), message.getBody))
                      deleteCallback.invoke(request)
                    }
                  }
                )
              case MessageAction.ChangeMessageVisibility(visibilityTimeout) =>
                sqsClient
                  .changeMessageVisibilityAsync(
                    new ChangeMessageVisibilityRequest(queueUrl, message.getReceiptHandle, visibilityTimeout),
                    new AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult] {

                      override def onError(exception: Exception): Unit = {
                        responsePromise.failure(exception)
                        failureCallback.invoke(exception)
                      }

                      override def onSuccess(request: ChangeMessageVisibilityRequest,
                                             result: ChangeMessageVisibilityResult): Unit = {
                        responsePromise.success(AckResult(Some(result), message.getBody))
                        changeVisibilityCallback.invoke(request)
                      }
                    }
                  )
              case MessageAction.Ignore =>
                responsePromise.success(AckResult(None, message.getBody))
            }
            push(out, responsePromise.future)
          }
        }
      )
    }
}
