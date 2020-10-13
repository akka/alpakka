/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.ArrayBlockingQueue

import akka.annotation.InternalApi
import akka.stream.alpakka.jms.{Destination, DurableTopic, StopMessageListenerException}
import javax.jms

import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API.
 */
@InternalApi
private[jms] sealed trait JmsSession {

  def connection: jms.Connection

  def session: jms.Session

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSession(): Unit = closeSession()
}

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsProducerSession(val connection: jms.Connection,
                                            val session: jms.Session,
                                            val jmsDestination: jms.Destination
) extends JmsSession

/**
 * Internal API.
 */
@InternalApi
private[jms] class JmsConsumerSession(val connection: jms.Connection,
                                      val session: jms.Session,
                                      val jmsDestination: jms.Destination,
                                      val settingsDestination: Destination
) extends JmsSession {

  private[jms] def createConsumer(
      selector: Option[String]
  )(implicit ec: ExecutionContext): Future[jms.MessageConsumer] =
    Future {
      (selector, settingsDestination) match {
        case (None, t: DurableTopic) =>
          session.createDurableSubscriber(jmsDestination.asInstanceOf[jms.Topic], t.subscriberName)

        case (Some(expr), t: DurableTopic) =>
          session.createDurableSubscriber(jmsDestination.asInstanceOf[jms.Topic], t.subscriberName, expr, false)

        case (Some(expr), _) =>
          session.createConsumer(jmsDestination, expr)

        case (None, _) =>
          session.createConsumer(jmsDestination)
      }
    }
}

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsAckSession(override val connection: jms.Connection,
                                       override val session: jms.Session,
                                       override val jmsDestination: jms.Destination,
                                       override val settingsDestination: Destination,
                                       val maxPendingAcks: Int
) extends JmsConsumerSession(connection, session, jmsDestination, settingsDestination) {

  private[jms] var pendingAck = 0
  private[jms] val ackQueue = new ArrayBlockingQueue[() => Unit](maxPendingAcks + 1)

  def ack(message: jms.Message): Unit = ackQueue.put(message.acknowledge _)

  override def closeSession(): Unit = stopMessageListenerAndCloseSession()

  override def abortSession(): Unit = stopMessageListenerAndCloseSession()

  private def stopMessageListenerAndCloseSession(): Unit = {
    ackQueue.put(() => throw StopMessageListenerException())
    session.close()
  }
}
