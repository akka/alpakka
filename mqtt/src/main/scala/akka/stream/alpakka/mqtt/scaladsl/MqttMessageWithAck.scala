/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.mqtt.MqttMessage

import scala.compat.java8.FutureConverters
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

  /**
   * Signals `messageArrivedComplete` to MQTT.
   *
   * @return a future indicating, if the acknowledge reached MQTT
   */
  def ack(): Future[Done]
}
/*
 * INTERNAL API
 */
@InternalApi
private[scaladsl] object MqttMessageWithAck {
  def fromJava(e: akka.stream.alpakka.mqtt.javadsl.MqttMessageWithAck): MqttMessageWithAck = new MqttMessageWithAck {
    override val message: MqttMessage = e.message

    /**
     * Signals `messageArrivedComplete` to MQTT.
     *
     * @return a future indicating, if the acknowledge reached MQTT
     */
    override def ack(): Future[Done] = FutureConverters.toScala(e.ack())
  }
}
