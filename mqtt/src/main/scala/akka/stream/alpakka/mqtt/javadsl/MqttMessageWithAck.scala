/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.mqtt.MqttMessage
import akka.stream.alpakka.mqtt.scaladsl

import scala.compat.java8.FutureConverters._

/**
 * Java API
 *
 * MQTT Message and a handle to acknowledge message reception to MQTT.
 */
sealed trait MqttMessageWithAck {

  /**
   * The message received from MQTT.
   */
  val message: MqttMessage

  /**
   * @deprecated use commit instead, since 0.21
   */
  @deprecated("use ack() instead", "1.0-M1")
  @java.lang.Deprecated
  def messageArrivedComplete(): CompletionStage[Done] = ack()

  /**
   * Signals `messageArrivedComplete` to MQTT.
   *
   * @return completion indicating, if the acknowledge reached MQTT
   */
  def ack(): CompletionStage[Done]
}

/**
 * INTERNAL API
 */
@InternalApi
private[javadsl] object MqttMessageWithAck {
  def toJava(cm: scaladsl.MqttMessageWithAck): MqttMessageWithAck = new MqttMessageWithAck {
    override val message: MqttMessage = cm.message
    override def ack(): CompletionStage[Done] = cm.ack().toJava
  }
}
