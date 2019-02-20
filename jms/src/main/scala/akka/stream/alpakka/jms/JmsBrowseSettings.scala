/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigValueType}

/**
 * Settings for [[akka.stream.alpakka.jms.scaladsl.JmsConsumer.browse]] and [[akka.stream.alpakka.jms.javadsl.JmsConsumer.browse]].
 */
final class JmsBrowseSettings private (
    val connectionFactory: javax.jms.ConnectionFactory,
    val connectionRetrySettings: ConnectionRetrySettings,
    val destination: Option[Destination],
    val credentials: Option[Credentials],
    val selector: Option[String],
    val acknowledgeMode: AcknowledgeMode
) extends akka.stream.alpakka.jms.JmsSettings {
  override val sessionCount = 1

  /** Factory to use for creating JMS connections. */
  def withConnectionFactory(value: javax.jms.ConnectionFactory): JmsBrowseSettings = copy(connectionFactory = value)

  /** Configure connection retrying. */
  def withConnectionRetrySettings(value: ConnectionRetrySettings): JmsBrowseSettings =
    copy(connectionRetrySettings = value)

  /** Set a queue name to browse from. */
  def withQueue(name: String): JmsBrowseSettings = copy(destination = Some(Queue(name)))

  /** Set a JMS to subscribe to. Allows for custom handling with [[akka.stream.alpakka.jms.CustomDestination CustomDestination]]. */
  def withDestination(value: Destination): JmsBrowseSettings = copy(destination = Option(value))

  /** Set JMS broker credentials. */
  def withCredentials(value: Credentials): JmsBrowseSettings = copy(credentials = Option(value))

  /**
   * JMS selector expression.
   *
   * @see https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html
   */
  def withSelector(value: String): JmsBrowseSettings = copy(selector = Option(value))

  /** Set an explicit acknowledge mode. (Consumers have specific defaults.) */
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
    s"acknowledgeMode=${AcknowledgeMode.asString(acknowledgeMode)}" +
    ")"
}

object JmsBrowseSettings {

  val configPath = "alpakka.jms.browse"

  /**
   * Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
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
      acknowledgeMode
    )
  }

  /**
   * Java API: Reads from the given config.
   *
   * @param c Config instance read configuration from
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(c: Config, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings = apply(c, connectionFactory)

  /**
   * Reads from the default config provided by the actor system at `alpakka.jms.browse`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def apply(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings =
    apply(actorSystem.settings.config.getConfig(configPath), connectionFactory)

  /**
   * Java API: Reads from the default config provided by the actor system at `alpakka.jms.browse`.
   *
   * @param actorSystem The actor system
   * @param connectionFactory Factory to use for creating JMS connections.
   */
  def create(actorSystem: ActorSystem, connectionFactory: javax.jms.ConnectionFactory): JmsBrowseSettings =
    apply(actorSystem, connectionFactory)

}
