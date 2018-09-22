/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.atomic.AtomicBoolean

import javax.jms
import javax.jms.{ConnectionFactory, Message}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

case class AckEnvelope private[jms] (message: Message, private val jmsSession: JmsAckSession) {

  val processed = new AtomicBoolean(false)

  def acknowledge(): Unit = if (processed.compareAndSet(false, true)) jmsSession.ack(message)
}

case class TxEnvelope private[jms] (message: Message, private val jmsSession: JmsSession) {

  private[this] val commitPromise = Promise[() => Unit]

  private[jms] val commitFuture: Future[() => Unit] = commitPromise.future

  def commit(): Unit = commitPromise.success(jmsSession.session.commit _)

  def rollback(): Unit = commitPromise.success(jmsSession.session.rollback _)
}

sealed trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def connectionRetrySettings: ConnectionRetrySettings
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def acknowledgeMode: Option[AcknowledgeMode]
  def sessionCount: Int
}

sealed trait Destination {
  val name: String
  val create: jms.Session => jms.Destination
}
final case class Topic(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}
final case class DurableTopic(name: String, subscriberName: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}
final case class Queue(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createQueue(name)
}
final case class CustomDestination(override val name: String, override val create: jms.Session => jms.Destination)
    extends Destination

final class AcknowledgeMode(val mode: Int)

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)
}

object ConnectionRetrySettings {
  def create(): ConnectionRetrySettings = ConnectionRetrySettings()
}

final case class ConnectionRetrySettings(connectTimeout: FiniteDuration = 10.seconds,
                                         initialRetry: FiniteDuration = 100.millis,
                                         backoffFactor: Double = 2,
                                         maxBackoff: FiniteDuration = 1.minute,
                                         maxRetries: Int = 10) {
  def withConnectTimeout(timeout: FiniteDuration): ConnectionRetrySettings = copy(connectTimeout = timeout)
  def withConnectTimeout(timeout: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(connectTimeout = Duration(timeout, unit))
  def withInitialRetry(delay: FiniteDuration): ConnectionRetrySettings = copy(initialRetry = delay)
  def withInitialRetry(delay: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(initialRetry = Duration(delay, unit))
  def withBackoffFactor(backoffFactor: Double): ConnectionRetrySettings = copy(backoffFactor = backoffFactor)
  def withMaxBackoff(maxBackoff: FiniteDuration): ConnectionRetrySettings = copy(maxBackoff = maxBackoff)
  def withMaxBackoff(maxBackoff: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(maxBackoff = Duration(maxBackoff, unit))
  def withMaxRetries(maxRetries: Int): ConnectionRetrySettings = copy(maxRetries = maxRetries)

  /** Hypothetical retry time, not accounting for maxBackoff. */
  def waitTime(retryNumber: Int): FiniteDuration =
    (initialRetry * Math.pow(retryNumber, backoffFactor)).asInstanceOf[FiniteDuration]
}

object JmsConsumerSettings {

  def create(connectionFactory: ConnectionFactory) = JmsConsumerSettings(connectionFactory)

}

final case class JmsConsumerSettings(connectionFactory: ConnectionFactory,
                                     connectionRetrySettings: ConnectionRetrySettings = ConnectionRetrySettings(),
                                     destination: Option[Destination] = None,
                                     credentials: Option[Credentials] = None,
                                     sessionCount: Int = 1,
                                     bufferSize: Int = 100,
                                     selector: Option[String] = None,
                                     acknowledgeMode: Option[AcknowledgeMode] = None,
                                     ackTimeout: Duration = 1.second,
                                     durableName: Option[String] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsConsumerSettings = copy(credentials = Some(credentials))
  def withConnectionRetrySettings(settings: ConnectionRetrySettings): JmsConsumerSettings =
    copy(connectionRetrySettings = settings)
  def withSessionCount(count: Int): JmsConsumerSettings = copy(sessionCount = count)
  def withBufferSize(size: Int): JmsConsumerSettings = copy(bufferSize = size)
  def withQueue(name: String): JmsConsumerSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsConsumerSettings = copy(destination = Some(Topic(name)))
  def withDurableTopic(name: String, subscriberName: String): JmsConsumerSettings =
    copy(destination = Some(DurableTopic(name, subscriberName)))
  def withDestination(destination: Destination): JmsConsumerSettings = copy(destination = Some(destination))
  def withSelector(selector: String): JmsConsumerSettings = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsConsumerSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
  def withAckTimeout(timeout: Duration): JmsConsumerSettings = copy(ackTimeout = timeout)
  def withAckTimeout(timeout: Long, unit: TimeUnit): JmsConsumerSettings =
    copy(ackTimeout = Duration(timeout, unit))
}

object JmsProducerSettings {

  def create(connectionFactory: ConnectionFactory) = JmsProducerSettings(connectionFactory)

}

final case class JmsProducerSettings(connectionFactory: ConnectionFactory,
                                     connectionRetrySettings: ConnectionRetrySettings = ConnectionRetrySettings(),
                                     destination: Option[Destination] = None,
                                     credentials: Option[Credentials] = None,
                                     sessionCount: Int = 1,
                                     timeToLive: Option[Duration] = None,
                                     acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsProducerSettings = copy(credentials = Some(credentials))
  def withConnectionRetrySettings(settings: ConnectionRetrySettings): JmsProducerSettings =
    copy(connectionRetrySettings = settings)
  def withSessionCount(count: Int): JmsProducerSettings = copy(sessionCount = count)
  def withQueue(name: String): JmsProducerSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsProducerSettings = copy(destination = Some(Topic(name)))
  def withDestination(destination: Destination): JmsProducerSettings = copy(destination = Some(destination))
  def withTimeToLive(ttl: java.time.Duration): JmsProducerSettings =
    copy(timeToLive = Some(Duration.fromNanos(ttl.toNanos)))
  def withTimeToLive(ttl: Duration): JmsProducerSettings = copy(timeToLive = Some(ttl))
  def withTimeToLive(ttl: Long, unit: TimeUnit): JmsProducerSettings = copy(timeToLive = Some(Duration(ttl, unit)))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsProducerSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

final case class Credentials(username: String, password: String) {
  override def toString = s"Credentials($username,${"*" * password.length})"
}

object JmsBrowseSettings {

  def create(connectionFactory: ConnectionFactory) = JmsBrowseSettings(connectionFactory)

}

final case class JmsBrowseSettings(connectionFactory: ConnectionFactory,
                                   connectionRetrySettings: ConnectionRetrySettings = ConnectionRetrySettings(),
                                   destination: Option[Destination] = None,
                                   credentials: Option[Credentials] = None,
                                   selector: Option[String] = None,
                                   acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  override val sessionCount = 1
  def withCredential(credentials: Credentials): JmsBrowseSettings = copy(credentials = Some(credentials))
  def withConnectionRetrySettings(settings: ConnectionRetrySettings): JmsBrowseSettings =
    copy(connectionRetrySettings = settings)
  def withQueue(name: String): JmsBrowseSettings = copy(destination = Some(Queue(name)))
  def withDestination(destination: Destination): JmsBrowseSettings = copy(destination = Some(destination))
  def withSelector(selector: String): JmsBrowseSettings = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode): JmsBrowseSettings =
    copy(acknowledgeMode = Option(acknowledgeMode))
}

final case class StopMessageListenerException() extends Exception("Stopping MessageListener.")
