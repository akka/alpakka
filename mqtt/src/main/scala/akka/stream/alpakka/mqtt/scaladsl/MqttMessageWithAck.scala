/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.MqttMessage

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT Message and a handle to acknowledge message reception to MQTT.
 */
trait MqttMessageWithAck {

  /**
   * The message received from MQTT.
   */
  val message: MqttMessage

  @deprecated("use ack() instead", "1.0-M1")
  def messageArrivedComplete(): Future[Done] = ack()

  /**
   * Signals `messageArrivedComplete` to MQTT.
   *
   * @return a future indicating, if the acknowledge reached MQTT
   */
  def ack(): Future[Done]
}
