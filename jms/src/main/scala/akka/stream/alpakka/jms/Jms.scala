/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import javax.jms
import javax.jms.{ConnectionFactory, Message}

case class AckEnvelope private[jms] (message: Message, private val jmsSession: JmsAckSession) {

  val processed = new AtomicBoolean(false)

  def acknowledge(): Unit = if (processed.compareAndSet(false, true)) jmsSession.ack(message)
}

case class TxEnvelope private[jms] (message: Message, private val jmsSession: JmsTxSession) {

  val processed = new AtomicBoolean(false)

  def commit(): Unit = if (processed.compareAndSet(false, true)) jmsSession.commit()

  def rollback(): Unit = if (processed.compareAndSet(false, true)) jmsSession.rollback()
}

sealed trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def acknowledgeMode: Option[AcknowledgeMode]
}

sealed trait Destination
final case class Topic(name: String) extends Destination
final case class Queue(name: String) extends Destination

final class AcknowledgeMode(val mode: Int)

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)
}

object JmsSourceSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSourceSettings(connectionFactory)

}

final case class JmsSourceSettings(connectionFactory: ConnectionFactory,
                                   destination: Option[Destination] = None,
                                   credentials: Option[Credentials] = None,
                                   sessionCount: Int = 1,
                                   bufferSize: Int = 100,
                                   selector: Option[String] = None,
                                   acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsSourceSettings = copy(credentials = Some(credentials))
  def withSessionCount(count: Int): JmsSourceSettings = copy(sessionCount = count)
  def withBufferSize(size: Int): JmsSourceSettings = copy(bufferSize = size)
  def withQueue(name: String): JmsSourceSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsSourceSettings = copy(destination = Some(Topic(name)))
  def withSelector(selector: String): JmsSourceSettings = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsSourceSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

object JmsSinkSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSinkSettings(connectionFactory)

}

final case class JmsSinkSettings(connectionFactory: ConnectionFactory,
                                 destination: Option[Destination] = None,
                                 credentials: Option[Credentials] = None,
                                 timeToLive: Option[Duration] = None,
                                 acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsSinkSettings = copy(credentials = Some(credentials))
  def withQueue(name: String): JmsSinkSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsSinkSettings = copy(destination = Some(Topic(name)))
  def withTimeToLive(ttl: Duration): JmsSinkSettings = copy(timeToLive = Some(ttl))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsSinkSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

final case class Credentials(username: String, password: String)
