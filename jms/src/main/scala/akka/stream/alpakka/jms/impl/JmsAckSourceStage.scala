/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl.JmsConnector.FlushAcknowledgementsTimerKey
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.OptionVal
import javax.jms

import scala.annotation.tailrec

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsAckSourceStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[AckEnvelope], JmsConsumerMatValue] {

  private val out = Outlet[AckEnvelope]("JmsSource.out")

  override protected def initialAttributes: Attributes = Attributes.name("JmsAckConsumer")

  override def shape: SourceShape[AckEnvelope] = SourceShape[AckEnvelope](out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new JmsAckSourceStageLogic(inheritedAttributes)
    (logic, logic.consumerControl)
  }

  private final class JmsAckSourceStageLogic(inheritedAttributes: Attributes)
      extends SourceStageLogic[AckEnvelope](shape, out, settings, destination, inheritedAttributes) {
    private val maxPendingAcks = settings.maxPendingAcks
    private val maxAckInterval = settings.maxAckInterval

    protected def createSession(connection: jms.Connection,
                                createDestination: jms.Session => javax.jms.Destination): JmsAckSession = {
      val session =
        connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.ClientAcknowledge).mode)
      new JmsAckSession(connection, session, createDestination(session), destination, maxPendingAcks)
    }

    protected def pushMessage(msg: AckEnvelope): Unit = push(out, msg)

    override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
      jmsSession match {
        case session: JmsAckSession =>
          maxAckInterval.foreach { timeout =>
            scheduleWithFixedDelay(FlushAcknowledgementsTimerKey(session), timeout, timeout)
          }
          session
            .createConsumer(settings.selector)
            .map { consumer =>
              consumer.setMessageListener(new jms.MessageListener {

                var listenerStopped = false

                def onMessage(message: jms.Message): Unit = {

                  @tailrec
                  def ackQueued(): Unit =
                    OptionVal(session.ackQueue.poll()) match {
                      case OptionVal.Some(action) =>
                        try {
                          action()
                          session.pendingAck -= 1
                        } catch {
                          case _: StopMessageListenerException =>
                            listenerStopped = true
                        }
                        if (!listenerStopped) ackQueued()
                      case OptionVal.None =>
                    }

                  if (!listenerStopped)
                    try {
                      handleMessage.invoke(AckEnvelope(message, session))
                      session.pendingAck += 1
                      if (session.pendingAck > maxPendingAcks) {
                        val action = session.ackQueue.take()
                        action()
                        session.pendingAck -= 1
                      }
                      ackQueued()
                    } catch {
                      case _: StopMessageListenerException =>
                        listenerStopped = true
                      case e: jms.JMSException =>
                        handleError.invoke(e)
                    }
                }
              })
            }
            .onComplete(sessionOpenedCB.invoke)

        case _ =>
          throw new IllegalArgumentException(
            "Session must be of type JMSAckSession, it is a " +
            jmsSession.getClass.getName
          )
      }
  }

}
