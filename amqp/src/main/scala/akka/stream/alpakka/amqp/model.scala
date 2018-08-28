/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

final class IncomingMessage private (
    val bytes: ByteString,
    val envelope: Envelope,
    val properties: BasicProperties
) {
  override def toString: String =
    s"IncomingMessage(bytes=$bytes, envelope=$envelope, properties=$properties)"
}

object IncomingMessage {
  def apply(bytes: ByteString, envelope: Envelope, properties: BasicProperties): IncomingMessage =
    new IncomingMessage(bytes, envelope, properties)

  /**
   * Java API
   */
  def create(bytes: ByteString, envelope: Envelope, properties: BasicProperties): IncomingMessage =
    IncomingMessage(bytes, envelope, properties)
}

final class OutgoingMessage private (val bytes: ByteString,
                                     val immediate: Boolean,
                                     val mandatory: Boolean,
                                     val properties: Option[BasicProperties] = None,
                                     val routingKey: Option[String] = None) {

  def withProperties(properties: BasicProperties): OutgoingMessage =
    copy(properties = Some(properties))

  def withRoutingKey(routingKey: String): OutgoingMessage =
    copy(routingKey = Some(routingKey))

  private def copy(properties: Option[BasicProperties] = properties, routingKey: Option[String] = routingKey) =
    new OutgoingMessage(bytes, immediate, mandatory, properties, routingKey)

  override def toString: String =
    s"OutgoingMessage(bytes=$bytes, immediate=$immediate, mandatory=$mandatory, properties=$properties, routingKey=$routingKey)"
}

object OutgoingMessage {
  def apply(bytes: ByteString, immediate: Boolean, mandatory: Boolean): OutgoingMessage =
    new OutgoingMessage(bytes, immediate, mandatory)

  /**
   * Java API
   */
  def create(bytes: ByteString, immediate: Boolean, mandatory: Boolean): OutgoingMessage =
    OutgoingMessage(bytes, immediate, mandatory)
}
