/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.Optional

import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

import scala.compat.java8.OptionConverters

final case class IncomingMessage(bytes: ByteString, envelope: Envelope, properties: BasicProperties)

final case class OutgoingMessage(bytes: ByteString,
                                 immediate: Boolean,
                                 mandatory: Boolean,
                                 props: Option[BasicProperties] = None,
                                 routingKey: Option[String] = None) {

  /**
   * Java API
   */
  def this(bytes: ByteString,
           immediate: Boolean,
           mandatory: Boolean,
           props: Optional[BasicProperties],
           routingKey: Optional[String]) =
    this(bytes, immediate, mandatory, OptionConverters.toScala(props), OptionConverters.toScala(routingKey))
}
