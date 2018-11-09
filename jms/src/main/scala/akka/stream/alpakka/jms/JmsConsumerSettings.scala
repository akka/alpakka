/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigValueType}
import akka.util.JavaDurationConverters._

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
    val durableName: Option[String]
) extends akka.stream.alpakka.jms.JmsSettings {

  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsConsumerSettings = copy(connectionFactory = value)
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsConsumerSettings =
    copy(connectionRetrySettings = value)
  def withQueue(name: String): JmsConsumerSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsConsumerSettings = copy(destination = Some(Topic(name)))
  def withDurableTopic(name: String, subscriberName: String): JmsConsumerSettings =
    copy(destination = Some(DurableTopic(name, subscriberName)))
  def withDestination(value: Destination): JmsConsumerSettings = copy(destination = Option(value))
  @deprecated("use withCredentials instead", "1.0-M1")
  def withCredential(value: Credentials): JmsConsumerSettings = copy(credentials = Option(value))
  def withCredentials(value: Credentials): JmsConsumerSettings = copy(credentials = Option(value))
  def withSessionCount(value: Int): JmsConsumerSettings = copy(sessionCount = value)
  def withBufferSize(value: Int): JmsConsumerSettings = copy(bufferSize = value)
  def withSelector(value: String): JmsConsumerSettings = copy(selector = Option(value))
  def withAcknowledgeMode(value: AcknowledgeMode): JmsConsumerSettings = copy(acknowledgeMode = Option(value))
  def withAckTimeout(value: scala.concurrent.duration.Duration): JmsConsumerSettings = copy(ackTimeout = value)
  def withAckTimeout(value: java.time.Duration): JmsConsumerSettings = copy(ackTimeout = value.asScala)
  def withDurableName(value: String): JmsConsumerSettings = copy(durableName = Option(value))

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
      durableName: Option[String] = durableName
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
    durableName = durableName
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
    s"acknowledgeMode=$acknowledgeMode," +
    s"ackTimeout=$ackTimeout," +
    s"durableName=$durableName" +
    ")"
}

object JmsConsumerSettings {

  val configPath = "alpakka.jms.consumer"

  /**
   * Reads from the given config.
   */
  def apply(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings = {
    def getOption[A](path: String, read: Config => A): Option[A] =
      if (c.hasPath(path) && (c.getValue(path).valueType() != ConfigValueType.STRING || c.getString(path) != "off"))
        Some(read(c))
      else None
    def getStringOption(path: String): Option[String] = if (c.hasPath(path)) Some(c.getString(path)) else None

    val connectionRetrySettings = ConnectionRetrySettings(c.getConfig("connection-retry"))
    val destination = None
    val credentials = getOption("credentials", c => Credentials(c.getConfig("credentials")))
    val sessionCount = c.getInt("session-count")
    val bufferSize = c.getInt("buffer-size")
    val selector = getStringOption("selector")
    val acknowledgeMode =
      getOption("acknowledge-mode", c => AcknowledgeMode.from(c.getString("acknowledge-mode")))
    val ackTimeout = c.getDuration("ack-timeout").asScala
    val durableName = getStringOption("durable-name")
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
      durableName
    )
  }

  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /** Java API */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(c, connectionFactory)

  /** Java API */
  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsConsumerSettings =
    apply(actorSystem, connectionFactory)
}
