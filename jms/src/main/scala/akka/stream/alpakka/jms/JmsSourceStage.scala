/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.util.concurrent.Semaphore
import javax.jms.{ TextMessage, _ }

import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler, StageLogging }
import akka.stream.{ Attributes, Outlet, SourceShape }

import scala.collection.mutable

class JmsSourceStage(settings: JmsSourceSettings) extends GraphStage[SourceShape[Message]] {

  val out = Outlet[Message]("JmsSource.out")
  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      override def jmsSettings = settings

      var jmsConsumer: Option[MessageConsumer] = None
      private val bufferSize = settings.bufferSize
      private val queue = mutable.Queue[Message]()
      private val backpressure = new Semaphore(bufferSize)

      val handleError = getAsyncCallback[Throwable](e => {
        fail(out, e)
      })

      val handleMessage = getAsyncCallback[Message](msg => {
        require(queue.size <= bufferSize)
        if (isAvailable(out)) {
          pushMessage(msg)
        } else {
          queue.enqueue(msg)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (queue.nonEmpty) {
            pushMessage(queue.dequeue())
          }
      })

      def pushMessage(msg: Message): Unit = {
        push(out, msg)
        backpressure.release()
      }

      override def preStart(): Unit = {
        super.preStart()
        try {
          jmsConsumer = for {
            session <- jmsSession
            queue <- jmsDestination
          } yield {
            val consumer = session.createConsumer(queue)
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
            consumer
          }
        } catch {
          case e: Exception =>
            settings.destination match {
              case Some(Queue(name)) =>
                log.error(e, "Error creating consumer on queue {}", name)
              case Some(Topic(name)) =>
                log.error(e, "Error creating consumer on topic {}", name)
              case _ =>
            }
            failStage(e)
        }
      }

    }

}
