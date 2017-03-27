/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import java.util.concurrent.atomic.AtomicInteger
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class SqsFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[String, Future[SendMessageResult]]] {

  private val in = Inlet[String]("messages")
  private val out = Outlet[Future[SendMessageResult]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    val logic = new GraphStageLogic(shape) with StageLogging {
      private var inFlight = new AtomicInteger(0)
      @volatile var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      override protected def logSource: Class[_] = classOf[SqsFlowStage]

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

          override def onPush() = {
            val msg = grab(in)
            val r = Promise[SendMessageResult]
            sqsClient.sendMessageAsync(
              new SendMessageRequest(queueUrl, msg),
              new AsyncHandler[SendMessageRequest, SendMessageResult] {

                override def onError(exception: Exception): Unit = {
                  r.failure(exception)
                  onComplete()
                }

                override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit = {
                  r.success(result)
                  onComplete()
                }

                def onComplete(): Unit =
                  if (inFlight.decrementAndGet() == 0 && inIsClosed)
                    checkForCompletionCB.invoke(())
              }
            )
            inFlight.incrementAndGet()
            push(out, r.future)

          }
        }
      )
    }
    logic
  }

}
