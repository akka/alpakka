/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.jms

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import com.typesafe.config.{Config, ConfigValueType}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

/**
 * Settings for [[akka.stream.alpakka.jms.scaladsl.JmsConsumer]] and [[akka.stream.alpakka.jms.javadsl.JmsConsumer]].
 */
final class JmsConsumerSettings private (
    val connectionFactory: javax.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val sessionCount: Int,
    val bufferSize: Int,
    val selector: Option[String],
    val acknowledgeMode: Option[AcknowledgeMode],
    val ackTimeout: scala.concurrent.duration.Duration,
    val maxAckInterval: Option[scala.concurrent.duration.FiniteDuration],
    val maxPendingAcks: Int,
    val failStreamOnAckTimeout: Boolean,
    val connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration
) extends akka.stream.alpakka.jms.JmsSettings {

  /** Factory to use for creating JMS connections. */
  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsConsumerSettings = copy(connectionFactory = value)

  /** Configure connection retrying. */
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsConsumerSettings =
    copy(connectionRetrySettings = value)

  /** Set a queue name to read from. */
  def withQueue(name: String): JmsConsumerSettings = copy(destination = Some(Queue(name)))

  /** Set a topic name to listen to. */
  def withTopic(name: String): JmsConsumerSettings = copy(destination = Some(Topic(name)))

  /** Set a durable topic name to listen to, with a unique subscriber name. */
  def withDurableTopic(name: String, subscriberName: String): JmsConsumerSettings =
    copy(destination = Some(DurableTopic(name, subscriberName)))

  /** Set a JMS to subscribe to. Allows for custom handling with [[akka.stream.alpakka.jms.CustomDestination CustomDestination]]. */
  def withDestination(value: Destination): JmsConsumerSettings = copy(destination = Option(value))

  /** Set JMS broker credentials. */
  def withCredentials(value: Credentials): JmsConsumerSettings = copy(credentials = Option(value))

  /**
   * Number of parallel sessions to use for receiving JMS messages.
   */
  def withSessionCount(value: Int): JmsConsumerSettings = copy(sessionCount = value)

  /** Buffer size for maximum number for messages read from JMS when there is no demand. */
  def withBufferSize(value: Int): JmsConsumerSettings = copy(bufferSize = value)

  /**
   * JMS selector expression.
   *
   * @see https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html
   */
  def withSelector(value: String): JmsConsumerSettings = copy(selector = Option(value))

  /** Set an explicit acknowledge mode. (Consumers have specific defaults.) */
  def withAcknowledgeMode(value: AcknowledgeMode): JmsConsumerSettings = copy(acknowledgeMode = Option(value))

  /** Timeout for acknowledge. (Used by TX consumers.) */
  def withAckTimeout(value: scala.concurrent.duration.Duration): JmsConsumerSettings = copy(ackTimeout = value)

  /** Java API: Timeout for acknowledge. (Used by TX consumers.) */
  def withAckTimeout(value: java.time.Duration): JmsConsumerSettings = copy(ackTimeout = value.toScala)

  /** Max interval before sending queued acknowledges back to the broker. (Used by AckSources.) */
  def withMaxAckInterval(value: scala.concurrent.duration.FiniteDuration): JmsConsumerSettings =
    copy(maxAckInterval = Option(value))

  /** Java API: Max interval before sending queued acknowledges back to the broker. (Used by AckSources.) */
  def withMaxAckInterval(value: java.time.Duration): JmsConsumerSettings =
    copy(maxAckInterval = Option(value.toScala))

  /** Max number of acks queued by AckSource before they are sent to broker. (Unless MaxAckInterval is specified) */
  def withMaxPendingAcks(value: Int): JmsConsumerSettings = copy(maxPendingAcks = value)

  /**
   * For use with transactions, if true the stream fails if Alpakka rolls back the transaction when `ackTimeout` is hit.
   */
  def withFailStreamOnAckTimeout(value: Boolean): JmsConsumerSettings =
    if (failStreamOnAckTimeout == value) this else copy(failStreamOnAckTimeout = value)

  /** Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: FiniteDuration): JmsConsumerSettings =
    copy(connectionStatusSubscriptionTimeout = value)

  /** Java API: Timeout for connection status subscriber */
  def withConnectionStatusSubscriptionTimeout(value: java.time.Duration): JmsConsumerSettings =
    copy(connectionStatusSubscriptionTimeout = value.toScala)

  private def copy(
      connectionFactory: javax.jms.ConnectionFactory = connectionFactory,
      connectionRetrySettings: ConnectionRetrySettings = connectionRetrySettings,
      destination: Option[Destination] = destination,
      credentials: Option[Credentials] = credentials,
      sessionCount: Int = sessionCount,
      bufferSize: Int = bufferSize,
      selector: Option[String] = selector,
      acknowledgeMode: Option[AcknowledgeMode] = acknowledgeMode,
      ackTimeout: scala.concurrent.duration.Duration = ackTimeout,
      maxAckInterval: Option[scala.concurrent.duration.FiniteDuration] = maxAckInterval,
      maxPendingAcks: Int = maxPendingAcks,
      failStreamOnAckTimeout: Boolean = failStreamOnAckTimeout,
      connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration =
        connectionStatusSubscriptionTimeout
  ): JmsConsumerSettings = new JmsConsumerSettings(
    connectionFactory = connectionFactory,
    connectionRetrySettings = connectionRetrySettings,
    destination = destination,
    credentials = credentials,
    sessionCount = sessionCount,
    bufferSize = bufferSize,
    selector = selector,
    acknowledgeMode = acknowledgeMode,
    ackTimeout = ackTimeout,
    maxAckInterval = maxAckInterval,
    maxPendingAcks = maxPendingAcks,
    failStreamOnAckTimeout = failStreamOnAckTimeout,
    connectionStatusSubscriptionTimeout = connectionStatusSubscriptionTimeout
  )

  override def toString =
    "JmsConsumerSettings(" +
    s"connectionFactory=$connectionFactory," +
    s"connectionRetrySettings=$connectionRetrySettings," +
    s"destination=$destination," +
    s"credentials=$credentials," +
    s"sessionCount=$sessionCount," +
    s"bufferSize=$bufferSize," +
    s"selector=$selector," +
    s"acknowledgeMode=${acknowledgeMode.map(m => AcknowledgeMode.asString(m))}," +
    s"ackTimeout=${ackTimeout.toCoarsest}," +
    s"maxAckInterval=${maxAckInterval.map(_.toCoarsest)}," +
    s"maxPendingAcks=$maxPendingAcks," +
    s"failStreamOnAckTimeout=$failStreamOnAckTimeout," +
    s"connectionStatusSubscriptionTimeout=${connectionStatusSubscriptionTimeout.toCoarsest}" +
    ")"
}

object JmsConsumerSettings {

  val configPath = "alpakka.jms.consumer"

  /**
   * Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings = {
    def getOption[A](path: String, read: Config => A): Option[A] =
      if (c.hasPath(path) && (c.getValue(path).valueType() != ConfigValueType.STRING || c.getString(path) != "off"))
        Some(read(c))
      else None

    def getStringOption(path: String): Option[String] =
      if (c.hasPath(path) && c.getString(path).nonEmpty) Some(c.getString(path)) else None

    val connectionRetrySettings = ConnectionRetrySettings(c.getConfig("connection-retry"))
    val destination = None
    val credentials = getOption("credentials", c => Credentials(c.getConfig("credentials")))
    val sessionCount = c.getInt("session-count")
    val bufferSize = c.getInt("buffer-size")
    val selector = getStringOption("selector")
    val acknowledgeMode = getOption("acknowledge-mode", c => AcknowledgeMode.from(c.getString("acknowledge-mode")))
    val ackTimeout = c.getDuration("ack-timeout").toScala
    val maxAckIntervalDuration = getOption("max-ack-interval", config => config.getDuration("max-ack-interval").toScala)
    val maxAckInterval = maxAckIntervalDuration.map(duration => FiniteDuration(duration.length, duration.unit))
    val maxPendingAcks = c.getInt("max-pending-acks")
    val failStreamOnAckTimeout = c.getBoolean("fail-stream-on-ack-timeout")
    val connectionStatusSubscriptionTimeout = c.getDuration("connection-status-subscription-timeout").toScala
    new JmsConsumerSettings(
      connectionFactory,
      connectionRetrySettings,
      destination,
      credentials,
      sessionCount,
      bufferSize,
      selector,
      acknowledgeMode,
      ackTimeout,
      maxAckInterval,
      maxPendingAcks,
      failStreamOnAckTimeout,
      connectionStatusSubscriptionTimeout
    )
  }

  /**
   * Reads from the default config provided by the actor system at `alpakka.jms.consumer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /**
   * Reads from the default config provided by the actor system at `alpakka.jms.consumer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ClassicActorSystemProvider,
            connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem.classicSystem, connectionFactory)

  /**
   * Java API: Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(c, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jms.consumer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem, connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jms.consumer`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ClassicActorSystemProvider,
             connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem.classicSystem, connectionFactory)
}
