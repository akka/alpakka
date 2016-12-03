/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.stream._
import akka.stream.stage._

import scala.concurrent.{ExecutionContext, Future}

class IronMqPushMessageStage(queue: Queue.Name, clientProvider: () => IronMqClient)
    extends GraphStage[FlowShape[PushMessage, Future[Message.Ids]]] {

  val in: Inlet[PushMessage] = Inlet("in")
  val out: Outlet[Future[Message.Ids]] = Outlet("out")

  override val shape: FlowShape[PushMessage, Future[Message.Ids]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      implicit def ec: ExecutionContext = materializer.executionContext

      override protected val logSource: Class[_] = classOf[IronMqPushMessageStage]

      private var runningFutures: Int = 0
      private var exceptionFromUpstream: Option[Throwable] = None

      private val client = clientProvider()

      setHandler(in,
        new InHandler {

        override def onPush(): Unit = {
          val pushMessage = grab(in)

          val future = client.pushMessages(queue, pushMessage)
          runningFutures = runningFutures + 1

          push(out, future)

          future.onComplete { _ =>
            futureCompleted.invoke(())
          }
        }

        override def onUpstreamFinish(): Unit =
          checkForCompletion()

        override def onUpstreamFailure(ex: Throwable): Unit = {
          exceptionFromUpstream = Some(ex)
          checkForCompletion()
        }
      })

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit =
          tryPull(in)
      })

      private def checkForCompletion() =
        if (isClosed(in) && runningFutures <= 0) {
          exceptionFromUpstream match {
            case None => completeStage()
            case Some(ex) => failStage(ex)
          }
        }

      private val futureCompleted = getAsyncCallback[Unit] { _ =>
        runningFutures = runningFutures - 1
        checkForCompletion()
      }

    }
}
