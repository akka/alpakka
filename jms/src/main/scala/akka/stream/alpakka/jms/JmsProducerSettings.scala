/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import akka.util.JavaDurationConverters._
import com.typesafe.config.{Config, ConfigValueType}

final class JmsProducerSettings private (
    val connectionFactory: javax.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val sendRetrySettings: SendRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val sessionCount: Int,
    val timeToLive: Option[scala.concurrent.duration.Duration]
) extends akka.stream.alpakka.jms.JmsSettings {

  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsProducerSettings = copy(connectionFactory = value)
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsProducerSettings =
    copy(connectionRetrySettings = value)
  def withSendRetrySettings(value: SendRetrySettings): JmsProducerSettings = copy(sendRetrySettings = value)
  def withQueue(name: String): JmsProducerSettings = copy(destination = Some(Queue(name)))
  def withTopic(name: String): JmsProducerSettings = copy(destination = Some(Topic(name)))
  def withDestination(value: Destination): JmsProducerSettings = copy(destination = Option(value))
  @deprecated("use withCredentials instead", "1.0-M1")
  def withCredential(value: Credentials): JmsProducerSettings = copy(credentials = Option(value))
  def withCredentials(value: Credentials): JmsProducerSettings = copy(credentials = Option(value))
  def withSessionCount(value: Int): JmsProducerSettings = copy(sessionCount = value)
  def withTimeToLive(value: scala.concurrent.duration.Duration): JmsProducerSettings = copy(timeToLive = Option(value))
  def withTimeToLive(value: java.time.Duration): JmsProducerSettings = copy(timeToLive = Option(value).map(_.asScala))

  private def copy(
      connectionFactory: javax.jms.ConnectionFactory = connectionFactory,
      connectionRetrySettings: ConnectionRetrySettings = connectionRetrySettings,
      sendRetrySettings: SendRetrySettings = sendRetrySettings,
      destination: Option[Destination] = destination,
      credentials: Option[Credentials] = credentials,
      sessionCount: Int = sessionCount,
      timeToLive: Option[scala.concurrent.duration.Duration] = timeToLive
  ): JmsProducerSettings = new JmsProducerSettings(
    connectionFactory = connectionFactory,
    connectionRetrySettings = connectionRetrySettings,
    sendRetrySettings = sendRetrySettings,
    destination = destination,
    credentials = credentials,
    sessionCount = sessionCount,
    timeToLive = timeToLive
  )

  override def toString =
    "JmsProducerSettings(" +
    s"connectionFactory=$connectionFactory," +
    s"connectionRetrySettings=$connectionRetrySettings," +
    s"sendRetrySettings=$sendRetrySettings," +
    s"destination=$destination," +
    s"credentials=$credentials," +
    s"sessionCount=$sessionCount," +
    s"timeToLive=$timeToLive" +
    ")"
}

object JmsProducerSettings {

  val configPath = "alpakka.jms.producer"

  /**
   * Reads from the given config.
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
    new JmsProducerSettings(
      connectionFactory,
      connectionRetrySettings,
      sendRetrySettings,
      destination = None,
      credentials,
      sessionCount,
      timeToLive
    )
  }

  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /** Java API */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(c, connectionFactory)

  /** Java API */
  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsProducerSettings =
    apply(actorSystem, connectionFactory)

}
