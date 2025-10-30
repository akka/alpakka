/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.jakartajms

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import com.typesafe.config.{Config, ConfigValueType}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

/**
 * Settings for [[akka.stream.alpakka.jakartajms.scaladsl.JmsProducer]] and [[akka.stream.alpakka.jakartajms.javadsl.JmsProducer]].
 */
final class JmsProducerSettings private (
    val connectionFactory: jakarta.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val sendRetrySettings: SendRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val sessionCount: Int,
    val timeToLive: Option[scala.concurrent.duration.Duration],
    val connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration
) extends akka.stream.alpakka.jakartajms.JmsSettings {

  /** Factory to use for creating JMS connections. */
  def withConnectionFactory(value: jakarta.jms.ConnectionFactory): JmsProducerSettings = copy(connectionFactory = value)

  /** Configure connection retrying. */
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsProducerSettings =
    copy(connectionRetrySettings = value)

  /** Configure re-sending. */
  def withSendRetrySettings(value: SendRetrySettings): JmsProducerSettings = copy(sendRetrySettings = value)

  /** Set a queue name as JMS destination. */
  def withQueue(name: String): JmsProducerSettings = copy(destination = Some(Queue(name)))

  /** Set a topic name as JMS destination. */
  def withTopic(name: String): JmsProducerSettings = copy(destination = Some(Topic(name)))

  /** Set a JMS destination. Allows for custom handling with [[akka.stream.alpakka.jakartajms.CustomDestination CustomDestination]]. */
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
  def withTimeToLive(value: java.time.Duration): JmsProducerSettings = copy(timeToLive = Option(value).map(_.toScala))

  /**  Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: FiniteDuration): JmsProducerSettings =
    copy(connectionStatusSubscriptionTimeout = value)

  /** Java API: Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: java.time.Duration): JmsProducerSettings =
    copy(connectionStatusSubscriptionTimeout = value.toScala)

  private def copy(
      connectionFactory: jakarta.jms.ConnectionFactory = connectionFactory,
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

  val configPath = "alpakka.jakarta-jms.producer"

  /**
   * Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(c: Config, connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings = {
    def getOption[A](path: String, read: Config => A): Option[A] =
      if (c.hasPath(path) && (c.getValue(path).valueType() != ConfigValueType.STRING || c.getString(path) != "off"))
        Some(read(c))
      else None

    val connectionRetrySettings = ConnectionRetrySettings(c.getConfig("connection-retry"))
    val sendRetrySettings = SendRetrySettings(c.getConfig("send-retry"))
    val credentials = getOption("credentials", c => Credentials(c.getConfig("credentials")))
    val sessionCount = c.getInt("session-count")
    val timeToLive = getOption("time-to-live", _.getDuration("time-to-live").toScala)
    val connectionStatusSubscriptionTimeout = c.getDuration("connection-status-subscription-timeout").toScala
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
   * Reads from the default config provided by the actor system at `alpakka.jakarta-jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ActorSystem, connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /**
   * Reads from the default config provided by the actor system at `alpakka.jakarta-jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ClassicActorSystemProvider,
            connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem.classicSystem, connectionFactory)

  /**
   * Java API: Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(c: Config, connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings =
    apply(c, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jakarta-jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ActorSystem, connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jakarta-jms.producer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ClassicActorSystemProvider,
             connectionFactory: jakarta.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem.classicSystem, connectionFactory)

}
