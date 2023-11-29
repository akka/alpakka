/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.jms.{AcknowledgeMode, Destination, JmsConsumerSettings, JmsTxAckTimeout, TxEnvelope}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import akka.stream.{Attributes, Outlet, SourceShape}
import javax.jms

import scala.concurrent.{Await, TimeoutException}

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsTxSourceStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[TxEnvelope], JmsConsumerMatValue] {

  private val out = Outlet[TxEnvelope]("JmsSource.out")

  override def shape: SourceShape[TxEnvelope] = SourceShape[TxEnvelope](out)

  override protected def initialAttributes: Attributes = Attributes.name("JmsTxConsumer")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new JmsTxSourceStageLogic(inheritedAttributes)
    (logic, logic.consumerControl)
  }

  private final class JmsTxSourceStageLogic(inheritedAttributes: Attributes)
      extends SourceStageLogic[TxEnvelope](shape, out, settings, destination, inheritedAttributes) { self =>
    protected def createSession(connection: jms.Connection, createDestination: jms.Session => javax.jms.Destination) = {
      val session =
        connection.createSession(true, settings.acknowledgeMode.getOrElse(AcknowledgeMode.SessionTransacted).mode)
      new JmsConsumerSession(connection, session, createDestination(session), self.destination)
    }

    protected def pushMessage(msg: TxEnvelope): Unit = push(out, msg)

    override protected def onSessionOpened(consumerSession: JmsConsumerSession): Unit =
      consumerSession
        .createConsumer(settings.selector)
        .map { consumer =>
          consumer.setMessageListener(new jms.MessageListener {
            def onMessage(message: jms.Message): Unit = {
              try {
                val envelope = TxEnvelope(message, consumerSession)
                handleMessage.invoke(envelope)
                try {
                  // JMS spec defines that commit/rollback must be done on the same thread.
                  // While some JMS implementations work without this constraint, IBM MQ is
                  // very strict about the spec and throws exceptions when called from a different thread.
                  val action = Await.result(envelope.commitFuture, settings.ackTimeout)
                  action()
                } catch {
                  case _: TimeoutException =>
                    val exception = new JmsTxAckTimeout(settings.ackTimeout)
                    consumerSession.session.rollback()
                    if (settings.failStreamOnAckTimeout) {
                      handleError.invoke(exception)
                    } else {
                      log.warning(exception.getMessage)
                    }
                }
              } catch {
                case e: IllegalArgumentException => handleError.invoke(e) // Invalid envelope, fail the stage
                case e: jms.JMSException => handleError.invoke(e)
              }
            }
          })
        }
        .onComplete(sessionOpenedCB.invoke)
  }

}
