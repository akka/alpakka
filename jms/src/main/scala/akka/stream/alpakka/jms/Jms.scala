/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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

object Destination {
  def createQueue(name: String)(session: jms.Session) = session.createQueue(name)
  def createTopic(name: String)(session: jms.Session) = session.createTopic(name)
}
sealed trait Destination
final case class Topic(name: String,
                       createDestination: (String) => (jms.Session) => jms.Destination = Destination.createTopic)
    extends Destination
final case class Queue(name: String,
                       createDestination: (String) => (jms.Session) => jms.Destination = Destination.createQueue)
    extends Destination

final class AcknowledgeMode(val mode: Int)

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)
}

object JmsConsumerSettings {

  def create(connectionFactory: ConnectionFactory) = JmsConsumerSettings(connectionFactory)

}

final case class JmsConsumerSettings(connectionFactory: ConnectionFactory,
                                     destination: Option[Destination] = None,
                                     credentials: Option[Credentials] = None,
                                     sessionCount: Int = 1,
                                     bufferSize: Int = 100,
                                     selector: Option[String] = None,
                                     acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsConsumerSettings = copy(credentials = Some(credentials))
  def withSessionCount(count: Int): JmsConsumerSettings = copy(sessionCount = count)
  def withBufferSize(size: Int): JmsConsumerSettings = copy(bufferSize = size)
  def withQueue(name: String): JmsConsumerSettings = copy(destination = Some(Queue(name)))
  def withQueue(
      name: String,
      createDestination: (String) => (jms.Session) => jms.Destination = Destination.createQueue
  ): JmsConsumerSettings = copy(destination = Some(Queue(name, createDestination)))
  def withTopic(name: String): JmsConsumerSettings = copy(destination = Some(Topic(name)))
  def withTopic(
      name: String,
      createDestination: (String) => (jms.Session) => jms.Destination = Destination.createTopic
  ): JmsConsumerSettings = copy(destination = Some(Topic(name, createDestination)))
  def withSelector(selector: String): JmsConsumerSettings = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsConsumerSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

object JmsProducerSettings {

  def create(connectionFactory: ConnectionFactory) = JmsProducerSettings(connectionFactory)

}

final case class JmsProducerSettings(connectionFactory: ConnectionFactory,
                                     destination: Option[Destination] = None,
                                     credentials: Option[Credentials] = None,
                                     timeToLive: Option[Duration] = None,
                                     acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsProducerSettings = copy(credentials = Some(credentials))
  def withQueue(name: String): JmsProducerSettings = copy(destination = Some(Queue(name)))
  def withQueue(name: String, createDestination: (String) => (jms.Session) => jms.Destination): JmsProducerSettings =
    copy(destination = Some(Queue(name, createDestination)))
  def withTopic(name: String): JmsProducerSettings = copy(destination = Some(Topic(name)))
  def withTopic(name: String, createDestination: (String) => (jms.Session) => jms.Destination): JmsProducerSettings =
    copy(destination = Some(Topic(name, createDestination)))
  def withTimeToLive(ttl: Duration): JmsProducerSettings = copy(timeToLive = Some(ttl))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsProducerSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

final case class Credentials(username: String, password: String)

object JmsBrowseSettings {

  def create(connectionFactory: ConnectionFactory) = JmsBrowseSettings(connectionFactory)

}

final case class JmsBrowseSettings(connectionFactory: ConnectionFactory,
                                   destination: Option[Queue] = None,
                                   credentials: Option[Credentials] = None,
                                   selector: Option[String] = None,
                                   acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsBrowseSettings = copy(credentials = Some(credentials))
  def withQueue(name: String): JmsBrowseSettings = copy(destination = Some(Queue(name)))
  def withQueue(name: String, createDestination: (String) => (jms.Session) => jms.Destination): JmsBrowseSettings =
    copy(destination = Some(Queue(name, createDestination)))
  def withSelector(selector: String): JmsBrowseSettings = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsBrowseSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

final case class StopMessageListenerException() extends Exception("Stopping MessageListener.")
