/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.{scaladsl, MqttMessage, MqttQoS, MqttSourceSettings}
import akka.stream.javadsl.Flow

import scala.compat.java8.FutureConverters._

/**
 * Java API
 *
 * MQTT flow factory.
 */
object MqttFlow {

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages (without a commit handle).
   *
   * @deprecated use atMostOnce() instead
   */
  @deprecated("use atMostOnce instead", "0.21")
  @java.lang.Deprecated
  def create(sourceSettings: MqttSourceSettings,
             bufferSize: Int,
             qos: MqttQoS): Flow[MqttMessage, MqttMessage, CompletionStage[Done]] =
    atMostOnce(sourceSettings, bufferSize, qos)

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages (without a commit handle).
   */
  def atMostOnce(settings: MqttSourceSettings,
                 bufferSize: Int,
                 qos: MqttQoS): Flow[MqttMessage, MqttMessage, CompletionStage[Done]] =
    scaladsl.MqttFlow
      .atMostOnce(settings, bufferSize, qos)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   */
  def atLeastOnce(
      settings: MqttSourceSettings,
      bufferSize: Int,
      qos: MqttQoS
  ): Flow[MqttMessage, MqttCommittableMessage, CompletionStage[Done]] =
    scaladsl.MqttFlow
      .atLeastOnce(settings, bufferSize, qos)
      .map(MqttCommittableMessage.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
}
