/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.Semaphore

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.jms.{Destination, _}
import akka.stream.stage._
import javax.jms._

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsConsumerStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[Message], JmsConsumerMatValue] {

  private val out = Outlet[Message]("JmsConsumer.out")

  override protected def initialAttributes: Attributes = Attributes.name("JmsConsumer")

  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new SourceStageLogic[Message](shape, out, settings, destination, inheritedAttributes) {

      private val bufferSize = (settings.bufferSize + 1) * settings.sessionCount

      private val backpressure = new Semaphore(bufferSize)

      protected def createSession(connection: Connection,
                                  createDestination: Session => javax.jms.Destination): JmsConsumerSession = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
        new JmsConsumerSession(connection, session, createDestination(session), destination)
      }

      protected def pushMessage(msg: Message): Unit = {
        push(out, msg)
        backpressure.release()
      }

      override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
        jmsSession
          .createConsumer(settings.selector)
          .map { consumer =>
            consumer.setMessageListener(new MessageListener {
              def onMessage(message: Message): Unit = {
                backpressure.acquire()
                handleMessage.invoke(message)
              }
            })
          }
          .onComplete(sessionOpenedCB.invoke)
    }

    (logic, logic.consumerControl)
  }
}
