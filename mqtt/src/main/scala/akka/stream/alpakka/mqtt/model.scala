/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

final class MqttMessage private (val topic: String,
                                 val payload: akka.util.ByteString,
                                 val qos: Option[MqttQoS],
                                 val retained: Boolean) {

  def withTopic(value: String): MqttMessage = copy(topic = value)
  def withPayload(value: akka.util.ByteString): MqttMessage = copy(payload = value)
  def withPayload(value: Array[Byte]): MqttMessage = copy(payload = akka.util.ByteString(value))
  def withQos(value: MqttQoS): MqttMessage = copy(qos = Option(value))
  def withRetained(value: Boolean): MqttMessage = if (retained == value) this else copy(retained = value)

  private def copy(topic: String = topic,
                   payload: akka.util.ByteString = payload,
                   qos: Option[MqttQoS] = qos,
                   retained: Boolean = retained): MqttMessage =
    new MqttMessage(topic = topic, payload = payload, qos = qos, retained = retained)

  override def toString =
    s"""MqttMessage(topic=$topic,payload=$payload,qos=$qos,retained=$retained)"""

  override def equals(other: Any): Boolean = other match {
    case that: MqttMessage =>
      java.util.Objects.equals(this.topic, that.topic) &&
      java.util.Objects.equals(this.payload, that.payload) &&
      java.util.Objects.equals(this.qos, that.qos) &&
      java.util.Objects.equals(this.retained, that.retained)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(topic, payload, qos, Boolean.box(retained))
}

object MqttMessage {

  /** Scala API */
  def apply(
      topic: String,
      payload: akka.util.ByteString
  ): MqttMessage = new MqttMessage(
    topic,
    payload,
    qos = None,
    retained = false
  )

  /** Java API */
  def create(
      topic: String,
      payload: akka.util.ByteString
  ): MqttMessage = new MqttMessage(
    topic,
    payload,
    qos = None,
    retained = false
  )
}
