/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

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
  def create(): AmqpSinkSettings = AmqpSinkSettings.create(AmqpConnectionLocal())
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
