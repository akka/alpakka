/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.scaladsl.Result
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private[sqs] final class SqsFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[String, Future[Result]]] {

  private val in = Inlet[String]("messages")
  private val out = Outlet[Future[Result]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0
      var inIsClosed = false
      var completionState: Option[Try[Unit]] = None

      override protected def logSource: Class[_] = classOf[SqsFlowStage]

      var failureCallback: AsyncCallback[Exception] = _
      var sendCallback: AsyncCallback[SendMessageResult] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[Exception](handleFailure)
        sendCallback = getAsyncCallback[SendMessageResult](handleSend)
      }

      private def handleFailure(exception: Exception): Unit = {
        log.error(exception, "Client failure: {}", exception.getMessage)
        inFlight -= 1
        failStage(exception)
        if (inFlight == 0 && inIsClosed)
          checkForCompletion()
      }

      private def handleSend(result: SendMessageResult): Unit = {
        log.debug(s"Sent message {}", result.getMessageId)
        inFlight -= 1
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
            inFlight += 1
            val msg = grab(in)
            val responsePromise = Promise[Result]

            sqsClient.sendMessageAsync(
              new SendMessageRequest(queueUrl, msg),
              new AsyncHandler[SendMessageRequest, SendMessageResult] {

                override def onError(exception: Exception): Unit = {
                  responsePromise.failure(exception)
                  failureCallback.invoke(exception)
                }

                override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit = {
                  responsePromise.success(Result(result, msg))
                  sendCallback.invoke(result)
                }
              }
            )
            push(out, responsePromise.future)
          }
        }
      )
    }
}
