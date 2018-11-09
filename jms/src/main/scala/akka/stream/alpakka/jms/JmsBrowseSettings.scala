/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigValueType}

final class JmsBrowseSettings private (
    val connectionFactory: javax.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val selector: Option[String],
    val acknowledgeMode: AcknowledgeMode
) extends akka.stream.alpakka.jms.JmsSettings {
  override val sessionCount = 1

  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsBrowseSettings = copy(connectionFactory = value)
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsBrowseSettings =
    copy(connectionRetrySettings = value)
  def withQueue(name: String): JmsBrowseSettings = copy(destination = Some(Queue(name)))
  def withDestination(value: Destination): JmsBrowseSettings = copy(destination = Option(value))
  def withCredentials(value: Credentials): JmsBrowseSettings = copy(credentials = Option(value))
  def withSelector(value: String): JmsBrowseSettings = copy(selector = Option(value))
  def withAcknowledgeMode(value: AcknowledgeMode): JmsBrowseSettings = copy(acknowledgeMode = value)

  private def copy(
      connectionFactory: javax.jms.ConnectionFactory = connectionFactory,
      connectionRetrySettings: ConnectionRetrySettings = connectionRetrySettings,
      destination: Option[Destination] = destination,
      credentials: Option[Credentials] = credentials,
      selector: Option[String] = selector,
      acknowledgeMode: AcknowledgeMode = acknowledgeMode
  ): JmsBrowseSettings = new JmsBrowseSettings(
    connectionFactory = connectionFactory,
    connectionRetrySettings = connectionRetrySettings,
    destination = destination,
    credentials = credentials,
    selector = selector,
    acknowledgeMode = acknowledgeMode
  )

  override def toString =
    "JmsBrowseSettings(" +
    s"connectionFactory=$connectionFactory," +
    s"connectionRetrySettings=$connectionRetrySettings," +
    s"destination=$destination," +
    s"credentials=$credentials," +
    s"selector=$selector," +
    s"acknowledgeMode=$acknowledgeMode" +
    ")"
}

object JmsBrowseSettings {

  val configPath = "alpakka.jms.browse"

  /**
   * Reads from the given config.
   */
  def apply(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings = {
    def getOption[A](path: String, read: Config => A): Option[A] =
      if (c.hasPath(path) && (c.getValue(path).valueType() != ConfigValueType.STRING || c.getString(path) != "off"))
        Some(read(c))
      else None
    def getStringOption(path: String): Option[String] =
      if (c.hasPath(path) && c.getString(path).nonEmpty) Some(c.getString(path)) else None

    val connectionRetrySettings = ConnectionRetrySettings(c.getConfig("connection-retry"))
    val destination = None
    val credentials = getOption("credentials", c => Credentials(c.getConfig("credentials")))
    val selector = getStringOption("selector")
    val acknowledgeMode = AcknowledgeMode.from(c.getString("acknowledge-mode"))
    new JmsBrowseSettings(
      connectionFactory,
      connectionRetrySettings,
      destination,
      credentials,
      selector,
      acknowledgeMode,
    )
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings = apply(c, connectionFactory)

  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings =
    apply(actorSystem, connectionFactory)

}
