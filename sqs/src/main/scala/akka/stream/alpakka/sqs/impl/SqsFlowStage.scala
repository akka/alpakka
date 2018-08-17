/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.Result
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[SendMessageRequest, Future[Result]]] {

  private val in = Inlet[SendMessageRequest]("messages")
  private val out = Outlet[Future[Result]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      var inIsClosed = false
      var completionState: Option[Try[Done]] = None

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
            inFlight += 1
            val msg = grab(in).withQueueUrl(queueUrl)
            val responsePromise = Promise[Result]

            val handler = new AsyncHandler[SendMessageRequest, SendMessageResult] {

              override def onError(exception: Exception): Unit = {
                responsePromise.failure(exception)
                failureCallback.invoke(exception)
              }

              override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit = {
                responsePromise.success(Result(result, msg.getMessageBody))
                sendCallback.invoke(result)
              }
            }
            sqsClient.sendMessageAsync(
              msg,
              handler
            )
            push(out, responsePromise.future)
          }
        }
      )
    }
}
