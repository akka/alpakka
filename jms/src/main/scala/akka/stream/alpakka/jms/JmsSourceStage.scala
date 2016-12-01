/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.util.concurrent.Semaphore
import javax.jms._

import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler, StageLogging }
import akka.stream.{ Attributes, Outlet, SourceShape }

import scala.collection.mutable
import scala.util.{ Failure, Success }

final class JmsSourceStage(settings: JmsSourceSettings) extends GraphStage[SourceShape[Message]] {

  private val out = Outlet[Message]("JmsSource.out")

  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      override private[jms] def jmsSettings = settings

      private val bufferSize = settings.bufferSize
      private val queue = mutable.Queue[Message]()
      private val backpressure = new Semaphore(bufferSize)

      private val handleError = getAsyncCallback[Throwable](e => {
        fail(out, e)
      })

      private val handleMessage = getAsyncCallback[Message](msg => {
        require(queue.size <= bufferSize)
        if (isAvailable(out)) {
          pushMessage(msg)
        } else {
          queue.enqueue(msg)
        }
      })

      private def pushMessage(msg: Message): Unit = {
        push(out, msg)
        backpressure.release()
      }

      override private[jms] def onSessionOpened(): Unit =
        jmsSession.createConsumer().onComplete {
          case Success(consumer) =>
            consumer.setMessageListener(new MessageListener {
              override def onMessage(message: Message): Unit = {
                backpressure.acquire()
                try {
                  message.acknowledge()
                  handleMessage.invoke(message)
                } catch {
                  case e: JMSException =>
                    backpressure.release()
                    handleError.invoke(e)
                }
              }
            })
          case Failure(e) =>
            settings.destination match {
              case Some(Queue(name)) =>
                log.error(e, "Error creating consumer on queue {}", name)
              case Some(Topic(name)) =>
                log.error(e, "Error creating consumer on topic {}", name)
              case _ =>
            }
            failStage(e)
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (queue.nonEmpty) {
            pushMessage(queue.dequeue())
          }
      })

      override def postStop(): Unit = {
        super.postStop()
        queue.clear()
      }
    }
}
