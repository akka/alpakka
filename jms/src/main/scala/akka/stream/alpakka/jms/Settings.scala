/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms

import scala.concurrent.duration._

sealed trait JmsSettings {
  def connectionFactory: jms.ConnectionFactory
  def connectionRetrySettings: ConnectionRetrySettings
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def acknowledgeMode: Option[AcknowledgeMode]
  def sessionCount: Int
}

object JmsConsumerSettings {

  def create(connectionFactory: jms.ConnectionFactory) = JmsConsumerSettings(connectionFactory)

}

final case class JmsConsumerSettings(connectionFactory: jms.ConnectionFactory,
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

  def create(connectionFactory: jms.ConnectionFactory) = JmsProducerSettings(connectionFactory)

}

final case class JmsProducerSettings(connectionFactory: jms.ConnectionFactory,
                                     connectionRetrySettings: ConnectionRetrySettings = ConnectionRetrySettings(),
                                     sendRetrySettings: SendRetrySettings = SendRetrySettings(),
                                     destination: Option[Destination] = None,
                                     credentials: Option[Credentials] = None,
                                     sessionCount: Int = 1,
                                     timeToLive: Option[Duration] = None,
                                     acknowledgeMode: Option[AcknowledgeMode] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials): JmsProducerSettings = copy(credentials = Some(credentials))
  def withConnectionRetrySettings(settings: ConnectionRetrySettings): JmsProducerSettings =
    copy(connectionRetrySettings = settings)
  def withSendRetrySettings(settings: SendRetrySettings): JmsProducerSettings =
    copy(sendRetrySettings = settings)
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

  def create(connectionFactory: jms.ConnectionFactory) = JmsBrowseSettings(connectionFactory)

}

final case class JmsBrowseSettings(connectionFactory: jms.ConnectionFactory,
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
