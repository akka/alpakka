/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import akka.util.ByteString

final case class MqttMessage(topic: String, payload: ByteString, qos: Option[MqttQoS] = None, retained: Boolean = false)

object MqttMessage {

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString) =
    MqttMessage(topic, payload)

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, qos: MqttQoS) =
    MqttMessage(topic, payload, Some(qos))

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, retained: Boolean) =
    MqttMessage(topic, payload, retained = retained)

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, qos: MqttQoS, retained: Boolean) =
    MqttMessage(topic, payload, Some(qos), retained = retained)
}
