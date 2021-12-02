/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.Semaphore

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.jms._
import akka.stream.stage._
import javax.jms

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsConsumerStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[jms.Message], JmsConsumerMatValue] {

  private val out = Outlet[jms.Message]("JmsConsumer.out")

  override protected def initialAttributes: Attributes = Attributes.name("JmsConsumer")

  override def shape: SourceShape[jms.Message] = SourceShape[jms.Message](out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new JmsConsumerStageLogic(inheritedAttributes)
    (logic, logic.consumerControl)
  }

  private final class JmsConsumerStageLogic(inheritedAttributes: Attributes)
      extends SourceStageLogic[jms.Message](shape, out, settings, destination, inheritedAttributes) {

    private val bufferSize = (settings.bufferSize + 1) * settings.sessionCount

    private val backpressure = new Semaphore(bufferSize)

    protected def createSession(connection: jms.Connection,
                                createDestination: jms.Session => javax.jms.Destination): JmsConsumerSession = {
      val session =
        connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
      new JmsConsumerSession(connection, session, createDestination(session), destination)
    }

    protected def pushMessage(msg: jms.Message): Unit = {
      push(out, msg)
      backpressure.release()
    }

    override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
      jmsSession
        .createConsumer(settings.selector)
        .map { consumer =>
          consumer.setMessageListener(new jms.MessageListener {
            def onMessage(message: jms.Message): Unit = {
              backpressure.acquire()
              handleMessage.invoke(message)
            }
          })
        }
        .onComplete(sessionOpenedCB.invoke)
  }
}
