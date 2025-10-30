/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.mqtt.MqttMessage

import scala.jdk.FutureConverters._
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
    override def ack(): Future[Done] = e.ack().asScala
  }
}
