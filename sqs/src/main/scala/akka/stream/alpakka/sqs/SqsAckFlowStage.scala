/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{
  DeleteMessageRequest,
  DeleteMessageResult,
  SendMessageRequest,
  SendMessageResult
}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class SqsAckFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[MessageActionPair, Future[AmazonWebServiceResult[ResponseMetadata]]]] {

  private val in = Inlet[MessageActionPair]("messages")
  private val out = Outlet[Future[AmazonWebServiceResult[ResponseMetadata]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    val logic = new GraphStageLogic(shape) with StageLogging {
      private var inFlight = new AtomicInteger(0)
      @volatile private var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      override protected def logSource: Class[_] = classOf[SqsAckFlowStage]

      def checkForCompletion() =
        if (isClosed(in) && inFlight.get == 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
        checkForCompletion()
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

          def onComplete(): Unit =
            if (inFlight.decrementAndGet() == 0 && inIsClosed)
              checkForCompletionCB.invoke(())

          override def onPush() = {

            def onComplete(): Unit =
              if (inFlight.decrementAndGet() == 0 && inIsClosed)
                checkForCompletionCB.invoke(())

            val (message, action) = grab(in)
            val r = Promise[AmazonWebServiceResult[ResponseMetadata]]
            action match {
              case Ack() =>
                sqsClient.deleteMessageAsync(
                  new DeleteMessageRequest(queueUrl, message.getReceiptHandle),
                  new AsyncHandler[DeleteMessageRequest, DeleteMessageResult] {

                    override def onError(exception: Exception): Unit = {
                      r.failure(exception)
                      onComplete()
                    }

                    override def onSuccess(request: DeleteMessageRequest, result: DeleteMessageResult): Unit = {
                      r.success(result)
                      onComplete()
                    }
                  }
                )
              case RequeueWithDelay(delaySeconds) =>
                sqsClient
                  .sendMessageAsync(
                    new SendMessageRequest(queueUrl, message.getBody).withDelaySeconds(delaySeconds),
                    new AsyncHandler[SendMessageRequest, SendMessageResult] {

                      override def onError(exception: Exception): Unit = {
                        r.tryFailure(exception)
                        onComplete()
                      }

                      override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit = {
                        r.success(result)
                        onComplete()
                      }
                    }
                  )
            }

            inFlight.incrementAndGet()
            push(out, r.future)

          }
        }
      )

    }
    logic

  }

}
