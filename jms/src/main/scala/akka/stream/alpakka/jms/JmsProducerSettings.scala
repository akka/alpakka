/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.util.JavaDurationConverters._
import com.typesafe.config.{Config, ConfigValueType}

import scala.concurrent.duration.FiniteDuration

/**
 * Settings for [[akka.stream.alpakka.jms.scaladsl.JmsProducer]] and [[akka.stream.alpakka.jms.javadsl.JmsProducer]].
 */
final class JmsProducerSettings private (
    val connectionFactory: javax.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val sendRetrySettings: SendRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val sessionCount: Int,
    val timeToLive: Option[scala.concurrent.duration.Duration],
    val connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration
) extends akka.stream.alpakka.jms.JmsSettings {

  /** Factory to use for creating JMS connections. */
  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsProducerSettings = copy(connectionFactory = value)

  /** Configure connection retrying. */
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsProducerSettings =
    copy(connectionRetrySettings = value)

  /** Configure re-sending. */
  def withSendRetrySettings(value: SendRetrySettings): JmsProducerSettings = copy(sendRetrySettings = value)

  /** Set a queue name as JMS destination. */
  def withQueue(name: String): JmsProducerSettings = copy(destination = Some(Queue(name)))

  /** Set a topic name as JMS destination. */
  def withTopic(name: String): JmsProducerSettings = copy(destination = Some(Topic(name)))

  /** Set a JMS destination. Allows for custom handling with [[akka.stream.alpakka.jms.CustomDestination CustomDestination]]. */
  def withDestination(value: Destination): JmsProducerSettings = copy(destination = Option(value))

  /** Set JMS broker credentials. */
  def withCredentials(value: Credentials): JmsProducerSettings = copy(credentials = Option(value))

  /**
   * Number of parallel sessions to use for sending JMS messages.
   * Increasing the number of parallel sessions increases throughput at the cost of message ordering.
   * While the messages may arrive out of order on the JMS broker, the producer flow outputs messages
   * in the order they are received.
   */
  def withSessionCount(value: Int): JmsProducerSettings = copy(sessionCount = value)

  /**
   * Time messages should be kept on the JMS broker. This setting can be overridden on
   * individual messages. If not set, messages will never expire.
   */
  def withTimeToLive(value: scala.concurrent.duration.Duration): JmsProducerSettings = copy(timeToLive = Option(value))

  /**
   * Java API: Time messages should be kept on the JMS broker. This setting can be overridden on
   * individual messages. If not set, messages will never expire.
   */
  def withTimeToLive(value: java.time.Duration): JmsProducerSettings = copy(timeToLive = Option(value).map(_.asScala))

  /** Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: FiniteDuration): JmsProducerSettings =
    copy(connectionStatusSubscriptionTimeout = value)

  /** Java API: Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: java.time.Duration): JmsProducerSettings =
    copy(connectionStatusSubscriptionTimeout = value.asScala)

  private def copy(
      connectionFactory: javax.jms.ConnectionFactory = connectionFactory,
      connectionRetrySettings: ConnectionRetrySettings = connectionRetrySettings,
      sendRetrySettings: SendRetrySettings = sendRetrySettings,
      destination: Option[Destination] = destination,
      credentials: Option[Credentials] = credentials,
      sessionCount: Int = sessionCount,
      timeToLive: Option[scala.concurrent.duration.Duration] = timeToLive,
      connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration =
        connectionStatusSubscriptionTimeout
  ): JmsProducerSettings = new JmsProducerSettings(
    connectionFactory = connectionFactory,
    connectionRetrySettings = connectionRetrySettings,
    sendRetrySettings = sendRetrySettings,
    destination = destination,
    credentials = credentials,
    sessionCount = sessionCount,
    timeToLive = timeToLive,
    connectionStatusSubscriptionTimeout = connectionStatusSubscriptionTimeout
  )

  override def toString =
    "JmsProducerSettings(" +
    s"connectionFactory=$connectionFactory," +
    s"connectionRetrySettings=$connectionRetrySettings," +
    s"sendRetrySettings=$sendRetrySettings," +
    s"destination=$destination," +
    s"credentials=$credentials," +
    s"sessionCount=$sessionCount," +
    s"timeToLive=${timeToLive.map(_.toCoarsest)}," +
    s"connectionStatusSubscriptionTimeout=${connectionStatusSubscriptionTimeout.toCoarsest}" +
    ")"
}

object JmsProducerSettings {

  val configPath = "alpakka.jms.producer"

  /**
   * Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings = {
    def getOption[A](path: String, read: Config => A): Option[A] =
      if (c.hasPath(path) && (c.getValue(path).valueType() != ConfigValueType.STRING || c.getString(path) != "off"))
        Some(read(c))
      else None

    val connectionRetrySettings = ConnectionRetrySettings(c.getConfig("connection-retry"))
    val sendRetrySettings = SendRetrySettings(c.getConfig("send-retry"))
    val credentials = getOption("credentials", c => Credentials(c.getConfig("credentials")))
    val sessionCount = c.getInt("session-count")
    val timeToLive = getOption("time-to-live", _.getDuration("time-to-live").asScala)
    val connectionStatusSubscriptionTimeout = c.getDuration("connection-status-subscription-timeout").asScala
    new JmsProducerSettings(
      connectionFactory,
      connectionRetrySettings,
      sendRetrySettings,
      destination = None,
      credentials,
      sessionCount,
      timeToLive,
      connectionStatusSubscriptionTimeout
    )
  }

  /**
   * Reads from the default config provided by the actor system at `alpakka.jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /**
   * Reads from the default config provided by the actor system at `alpakka.jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ClassicActorSystemProvider,
            connectionFactory: javax.jms.ConnectionFactory
  ): JmsProducerSettings =
    apply(actorSystem.classicSystem, connectionFactory)

  /**
   * Java API: Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(c, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ClassicActorSystemProvider,
             connectionFactory: javax.jms.ConnectionFactory
  ): JmsProducerSettings =
    apply(actorSystem.classicSystem, connectionFactory)

}
