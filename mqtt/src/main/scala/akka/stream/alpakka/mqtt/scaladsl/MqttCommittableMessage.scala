/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.MqttMessage

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT Message and a handle to commit message reception to MQTT.
 */
trait MqttCommittableMessage {

  /**
   * The message received from MQTT.
   */
  val message: MqttMessage

  @deprecated("use commit instead", "0.21")
  def messageArrivedComplete(): Future[Done] = commit()

  /**
   * Signals `messageArrivedComplete` to MQTT.
   *
   * @return a future indicating, if the commit reached MQTT
   */
  def commit(): Future[Done]
}
