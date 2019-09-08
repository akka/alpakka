/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.Objects

import akka.NotUsed
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

final class ReadResult private (
    val bytes: ByteString,
    val envelope: Envelope,
    val properties: BasicProperties
) {
  override def toString: String =
    s"ReadResult(bytes=$bytes, envelope=$envelope, properties=$properties)"
}

object ReadResult {
  def apply(bytes: ByteString, envelope: Envelope, properties: BasicProperties): ReadResult =
    new ReadResult(bytes, envelope, properties)

  /**
   * Java API
   */
  def create(bytes: ByteString, envelope: Envelope, properties: BasicProperties): ReadResult =
    ReadResult(bytes, envelope, properties)
}

final class WriteMessage[PT] private (
    val bytes: ByteString,
    val immediate: Boolean,
    val mandatory: Boolean,
    val passThrough: PT = NotUsed,
    val properties: Option[BasicProperties] = None,
    val routingKey: Option[String] = None
) {

  def withImmediate(value: Boolean): WriteMessage[PT] =
    if (value == immediate) this
    else copy(immediate = value)

  def withMandatory(value: Boolean): WriteMessage[PT] =
    if (value == mandatory) this
    else copy(mandatory = value)

  def withPassThrough[PT2](passThrough: PT2): WriteMessage[PT2] =
    new WriteMessage[PT2](
      bytes = bytes,
      immediate = immediate,
      mandatory = mandatory,
      passThrough = passThrough,
      properties = properties,
      routingKey = routingKey
    )

  def withProperties(properties: BasicProperties): WriteMessage[PT] =
    copy(properties = Some(properties))

  def withRoutingKey(routingKey: String): WriteMessage[PT] =
    copy(routingKey = Some(routingKey))

  private def copy(immediate: Boolean = immediate,
                   mandatory: Boolean = mandatory,
                   properties: Option[BasicProperties] = properties,
                   routingKey: Option[String] = routingKey) =
    new WriteMessage(bytes, immediate, mandatory, passThrough, properties, routingKey)

  override def toString: String =
    "WriteMessage(" +
    s"bytes=$bytes, " +
    s"immediate=$immediate, " +
    s"mandatory=$mandatory, " +
    s"passThrough=$passThrough, " +
    s"properties=$properties, " +
    s"routingKey=$routingKey" +
    ")"
}

object WriteMessage {
  def apply(bytes: ByteString): WriteMessage[NotUsed] =
    new WriteMessage(bytes, immediate = false, mandatory = false)

  def apply(bytes: ByteString, immediate: Boolean, mandatory: Boolean): WriteMessage[NotUsed] =
    new WriteMessage(bytes, immediate, mandatory)

  /**
   * Java API
   */
  def create(bytes: ByteString): WriteMessage[NotUsed] = WriteMessage(bytes)

  /**
   * Java API
   */
  def create(bytes: ByteString, immediate: Boolean, mandatory: Boolean): WriteMessage[NotUsed] =
    WriteMessage(bytes, immediate, mandatory)
}

final class WriteResult[T] private (val confirmed: Boolean, val passThrough: T) {
  def rejected: Boolean = !confirmed

  override def toString: String =
    s"WriteResult(confirmed=$confirmed, passThrough=$passThrough)"

  override def equals(other: Any): Boolean = other match {
    case that: WriteResult[T] =>
      Objects.equals(this.confirmed, that.confirmed) &&
      Objects.equals(this.passThrough, that.passThrough)
    case _ => false
  }

  override def hashCode(): Int =
    passThrough match {
      case pt: AnyRef =>
        Objects.hash(Boolean.box(confirmed), pt)
      case _ =>
        Objects.hash(Boolean.box(confirmed))
    }
}

object WriteResult {
  def apply[T](confirmed: Boolean, passThrough: T): WriteResult[T] =
    new WriteResult(confirmed, passThrough)

  /**
   * Java API
   */
  def create[T](confirmed: Boolean, passThrough: T): WriteResult[T] =
    WriteResult(confirmed, passThrough)

  def confirmed[T](passThrough: T): WriteResult[T] =
    WriteResult(confirmed = true, passThrough)

  def rejected[T](passThrough: T): WriteResult[T] =
    WriteResult(confirmed = false, passThrough)
}
