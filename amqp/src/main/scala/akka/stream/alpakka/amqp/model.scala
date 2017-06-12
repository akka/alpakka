/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import com.rabbitmq.client.ExceptionHandler
import scala.collection.JavaConverters._

/**
 * Internal API
 */
sealed trait AmqpConnectorSettings {
  def connectionSettings: AmqpConnectionSettings
  def declarations: Seq[Declaration]
}

sealed trait AmqpSourceSettings extends AmqpConnectorSettings

final case class NamedQueueSourceSettings(
    connectionSettings: AmqpConnectionSettings,
    queue: String,
    declarations: Seq[Declaration] = Seq.empty,
    noLocal: Boolean = false,
    exclusive: Boolean = false,
    consumerTag: String = "default",
    arguments: Map[String, AnyRef] = Map.empty
) extends AmqpSourceSettings {
  @annotation.varargs
  def withDeclarations(declarations: Declaration*) = copy(declarations = declarations.toList)

  def withNoLocal(noLocal: Boolean) = copy(noLocal = noLocal)

  def withExclusive(exclusive: Boolean) = copy(exclusive = exclusive)

  def withConsumerTag(consumerTag: String) = copy(consumerTag = consumerTag)

  def withArguments(argument: (String, AnyRef), arguments: (String, AnyRef)*) =
    copy(arguments = (argument +: arguments).toMap)

  @annotation.varargs
  def withArguments(argument: akka.japi.Pair[String, AnyRef], arguments: akka.japi.Pair[String, AnyRef]*) =
    copy(arguments = (argument +: arguments).map(_.toScala).toMap)
}

object NamedQueueSourceSettings {

  /**
   * Java API
   */
  def create(connectionSettings: AmqpConnectionSettings, queue: String): NamedQueueSourceSettings =
    NamedQueueSourceSettings(connectionSettings, queue)
}

final case class TemporaryQueueSourceSettings(
    connectionSettings: AmqpConnectionSettings,
    exchange: String,
    declarations: Seq[Declaration] = Seq.empty,
    routingKey: Option[String] = None
) extends AmqpSourceSettings {
  def withRoutingKey(routingKey: String) = copy(routingKey = Some(routingKey))

  @annotation.varargs
  def withDeclarations(declarations: Declaration*) = copy(declarations = declarations.toList)
}

object TemporaryQueueSourceSettings {

  /**
   * Java API
   */
  def create(connectionSettings: AmqpConnectionSettings, exchange: String): TemporaryQueueSourceSettings =
    TemporaryQueueSourceSettings(connectionSettings, exchange)
}

final case class AmqpReplyToSinkSettings(
    connectionSettings: AmqpConnectionSettings,
    failIfReplyToMissing: Boolean = true
) extends AmqpConnectorSettings {
  override final val declarations = Nil
}

object AmqpReplyToSinkSettings {

  /**
   * Java API
   */
  def create(connectionSettings: AmqpConnectionSettings): AmqpReplyToSinkSettings =
    AmqpReplyToSinkSettings(connectionSettings)

  /**
   * Java API
   */
  def create(connectionSettings: AmqpConnectionSettings, failIfReplyToMissing: Boolean): AmqpReplyToSinkSettings =
    AmqpReplyToSinkSettings(connectionSettings, failIfReplyToMissing)

}

final case class AmqpSinkSettings(
    connectionSettings: AmqpConnectionSettings,
    exchange: Option[String] = None,
    routingKey: Option[String] = None,
    declarations: Seq[Declaration] = Seq.empty
) extends AmqpConnectorSettings {
  def withExchange(exchange: String) = copy(exchange = Some(exchange))

  def withRoutingKey(routingKey: String) = copy(routingKey = Some(routingKey))

  @annotation.varargs
  def withDeclarations(declarations: Declaration*) = copy(declarations = declarations.toList)
}

object AmqpSinkSettings {

  /**
   * Java API
   */
  def create(connectionSettings: AmqpConnectionSettings): AmqpSinkSettings =
    AmqpSinkSettings(connectionSettings)

  /**
   * Java API
   */
  def create(): AmqpSinkSettings = AmqpSinkSettings.create(DefaultAmqpConnection)
}

/**
 * Only for internal implementations
 */
sealed trait AmqpConnectionSettings

/**
 * Connects to a local AMQP broker at the default port with no password.
 */
case object DefaultAmqpConnection extends AmqpConnectionSettings {

  /**
   * Java API
   */
  def getInstance(): DefaultAmqpConnection.type = this
}

final case class AmqpConnectionUri(uri: String) extends AmqpConnectionSettings

object AmqpConnectionUri {

  /**
   * Java API:
   */
  def create(uri: String): AmqpConnectionUri = AmqpConnectionUri(uri)
}

final case class AmqpConnectionDetails(
    hostAndPortList: Seq[(String, Int)],
    credentials: Option[AmqpCredentials] = None,
    virtualHost: Option[String] = None,
    sslProtocol: Option[String] = None,
    requestedHeartbeat: Option[Int] = None,
    connectionTimeout: Option[Int] = None,
    handshakeTimeout: Option[Int] = None,
    shutdownTimeout: Option[Int] = None,
    networkRecoveryInterval: Option[Int] = None,
    automaticRecoveryEnabled: Option[Boolean] = None,
    topologyRecoveryEnabled: Option[Boolean] = None,
    exceptionHandler: Option[ExceptionHandler] = None
) extends AmqpConnectionSettings {

  def withHostsAndPorts(hostAndPort: (String, Int), hostAndPorts: (String, Int)*): AmqpConnectionDetails =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).toList)

  def withCredentials(amqpCredentials: AmqpCredentials): AmqpConnectionDetails =
    copy(credentials = Option(amqpCredentials))

  def withVirtualHost(virtualHost: String): AmqpConnectionDetails =
    copy(virtualHost = Option(virtualHost))

  def withSslProtocol(sslProtocol: String): AmqpConnectionDetails =
    copy(sslProtocol = Option(sslProtocol))

  def withRequestedHeartbeat(requestedHeartbeat: Int): AmqpConnectionDetails =
    copy(requestedHeartbeat = Option(requestedHeartbeat))

  def withConnectionTimeout(connectionTimeout: Int): AmqpConnectionDetails =
    copy(connectionTimeout = Option(connectionTimeout))

  def withHandshakeTimeout(handshakeTimeout: Int): AmqpConnectionDetails =
    copy(handshakeTimeout = Option(handshakeTimeout))

  def withShutdownTimeout(shutdownTimeout: Int): AmqpConnectionDetails =
    copy(shutdownTimeout = Option(shutdownTimeout))

  def withNetworkRecoveryInterval(networkRecoveryInterval: Int): AmqpConnectionDetails =
    copy(networkRecoveryInterval = Option(networkRecoveryInterval))

  def withAutomaticRecoveryEnabled(automaticRecoveryEnabled: Boolean): AmqpConnectionDetails =
    copy(automaticRecoveryEnabled = Option(automaticRecoveryEnabled))

  def withTopologyRecoveryEnabled(topologyRecoveryEnabled: Boolean): AmqpConnectionDetails =
    copy(topologyRecoveryEnabled = Option(topologyRecoveryEnabled))

  def withExceptionHandler(exceptionHandler: ExceptionHandler): AmqpConnectionDetails =
    copy(exceptionHandler = Option(exceptionHandler))

  /**
   * Java API:
   */
  @annotation.varargs
  def withHostsAndPorts(hostAndPort: akka.japi.Pair[String, Int],
                        hostAndPorts: akka.japi.Pair[String, Int]*): AmqpConnectionDetails =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).map(_.toScala).toList)
}

object AmqpConnectionDetails {

  def apply(host: String, port: Int): AmqpConnectionDetails =
    AmqpConnectionDetails(List((host, port)))

  /**
   * Java API:
   */
  def create(host: String, port: Int): AmqpConnectionDetails =
    AmqpConnectionDetails(host, port)
}

final case class AmqpCredentials(username: String, password: String) {
  override def toString = s"Credentials($username, ********)"
}

object AmqpCredentials {

  /**
   * Java API
   */
  def create(username: String, password: String): AmqpCredentials =
    AmqpCredentials(username, password)
}

sealed trait Declaration

final case class QueueDeclaration(
    name: String,
    durable: Boolean = false,
    exclusive: Boolean = false,
    autoDelete: Boolean = false,
    arguments: Map[String, AnyRef] = Map.empty
) extends Declaration {
  def withDurable(durable: Boolean) = copy(durable = durable)

  def withExclusive(exclusive: Boolean) = copy(exclusive = exclusive)

  def withAutoDelete(autoDelete: Boolean) = copy(autoDelete = autoDelete)

  def withArguments(argument: (String, AnyRef), arguments: (String, AnyRef)*) =
    copy(arguments = (argument +: arguments).toMap)

  @annotation.varargs
  def withArguments(argument: akka.japi.Pair[String, AnyRef], arguments: akka.japi.Pair[String, AnyRef]*) =
    copy(arguments = (argument +: arguments).map(_.toScala).toMap)
}

object QueueDeclaration {

  /**
   * Java API
   */
  def create(name: String): QueueDeclaration = QueueDeclaration(name)
}

final case class BindingDeclaration(
    queue: String,
    exchange: String,
    routingKey: Option[String] = None,
    arguments: Map[String, AnyRef] = Map.empty
) extends Declaration {
  def withRoutingKey(routingKey: String) = copy(routingKey = Some(routingKey))

  def withArguments(argument: (String, AnyRef), arguments: (String, AnyRef)*) =
    copy(arguments = (argument +: arguments).toMap)

  @annotation.varargs
  def withArguments(argument: akka.japi.Pair[String, AnyRef], arguments: akka.japi.Pair[String, AnyRef]*) =
    copy(arguments = (argument +: arguments).map(_.toScala).toMap)
}

object BindingDeclaration {

  /**
   * Java API
   */
  def create(queue: String, exchange: String): BindingDeclaration = BindingDeclaration(queue, exchange)
}

final case class ExchangeDeclaration(
    name: String,
    exchangeType: String,
    durable: Boolean = false,
    autoDelete: Boolean = false,
    internal: Boolean = false,
    arguments: Map[String, AnyRef] = Map.empty
) extends Declaration {
  def withDurable(durable: Boolean) = copy(durable = durable)

  def withAutoDelete(autoDelete: Boolean) = copy(autoDelete = autoDelete)

  def withInternal(internal: Boolean) = copy(internal = internal)

  def withArguments(argument: (String, AnyRef), arguments: (String, AnyRef)*) =
    copy(arguments = (argument +: arguments).toMap)

  @annotation.varargs
  def withArguments(argument: akka.japi.Pair[String, AnyRef], arguments: akka.japi.Pair[String, AnyRef]*) =
    copy(arguments = (argument +: arguments).map(_.toScala).toMap)
}

object ExchangeDeclaration {

  /**
   * Java API
   */
  def create(name: String, exchangeType: String): ExchangeDeclaration = ExchangeDeclaration(name, exchangeType)
}
