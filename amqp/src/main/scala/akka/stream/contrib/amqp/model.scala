/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp

import scala.collection.immutable.Seq

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
  queue:              String,
  declarations:       Seq[Declaration],
  noLocal:            Boolean                = false,
  exclusive:          Boolean                = false,
  consumerTag:        String                 = "default",
  arguments:          Map[String, AnyRef]    = Map.empty
) extends AmqpSourceSettings

final case class TemporaryQueueSourceSettings(
  connectionSettings: AmqpConnectionSettings,
  exchange:           String,
  declarations:       Seq[Declaration],
  routingKey:         Option[String]         = None
) extends AmqpSourceSettings

final case class AmqpSinkSettings(
  connectionSettings: AmqpConnectionSettings,
  exchange:           Option[String],
  routingKey:         Option[String],
  declarations:       Seq[Declaration]
) extends AmqpConnectorSettings {
}

/**
 * Only for internal implementations
 */
sealed trait AmqpConnectionSettings

case object DefaultAmqpConnection extends AmqpConnectionSettings

final case class AmqpConnectionUri(uri: String) extends AmqpConnectionSettings
final case class AmqpConnectionDetails(
  host:        String,
  port:        Int,
  credentials: Option[AmqpCredentials] = None,
  virtualHost: Option[String]          = None
) extends AmqpConnectionSettings
final case class AmqpCredentials(username: String, password: String) {
  override def toString = s"Credentials($username, ********)"
}

sealed trait Declaration

final case class QueueDeclaration(
  name:       String,
  durable:    Boolean             = false,
  exclusive:  Boolean             = false,
  autoDelete: Boolean             = false,
  arguments:  Map[String, AnyRef] = Map.empty
) extends Declaration

final case class BindingDeclaration(
  queue:      String,
  exchange:   String,
  routingKey: Option[String]      = None,
  arguments:  Map[String, AnyRef] = Map.empty
) extends Declaration

final case class ExchangeDeclaration(
  name:         String,
  exchangeType: String,
  durable:      Boolean             = false,
  autoDelete:   Boolean             = false,
  internal:     Boolean             = false,
  arguments:    Map[String, AnyRef] = Map.empty
) extends Declaration