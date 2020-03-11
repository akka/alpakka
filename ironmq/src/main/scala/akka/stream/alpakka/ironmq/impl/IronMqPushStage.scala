/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.ironmq._
import akka.stream.stage._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API.
 *
 * It is a very trivial IronMQ push stage. It push the message to IronMq as soon as they are pushed to this Stage.
 *
 * Because of that it does not guarantee the order of the produced messages and does not apply any backpressure. A more
 * sophisticated implementation will buffer the messages before pushing them and allow only a certain amount of parallel
 * requests.
 */
@InternalApi
private[ironmq] class IronMqPushStage(queueName: String, settings: IronMqSettings)
    extends GraphStage[FlowShape[PushMessage, Future[Message.Ids]]] {

  val in: Inlet[PushMessage] = Inlet("IronMqPush.in")
  val out: Outlet[Future[Message.Ids]] = Outlet("IronMqPush.out")

  override protected def initialAttributes: Attributes = Attributes.name("IronMqPush")

  override val shape: FlowShape[PushMessage, Future[Message.Ids]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      implicit def ec: ExecutionContext = materializer.executionContext

      override protected val logSource: Class[_] = classOf[IronMqPushStage]

      private var runningFutures: Int = 0
      private var exceptionFromUpstream: Option[Throwable] = None
      private var client: IronMqClient = _ // set in preStart

      override def preStart(): Unit = {
        super.preStart()
        client = IronMqClient(settings)(ActorMaterializerHelper.downcast(materializer).system, materializer)
      }

      setHandler(
        in,
        new InHandler {

          override def onPush(): Unit = {
            val pushMessage = grab(in)

            val future = client.pushMessages(queueName, pushMessage)
            runningFutures = runningFutures + 1
            setKeepGoing(true)

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
        }
      )

      setHandler(out, new OutHandler {
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
