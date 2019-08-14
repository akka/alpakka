/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt._
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
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atMostOnce(settings: MqttConnectionSettings,
                 subscriptions: MqttSubscriptions,
                 bufferSize: Int,
                 defaultQos: MqttQoS): Flow[MqttMessage, MqttMessage, CompletionStage[Done]] =
    scaladsl.MqttFlow
      .atMostOnce(settings, subscriptions, bufferSize, defaultQos)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atLeastOnce(
      settings: MqttConnectionSettings,
      subscriptions: MqttSubscriptions,
      bufferSize: Int,
      defaultQos: MqttQoS
  ): Flow[MqttMessage, MqttMessageWithAck, CompletionStage[Done]] =
    scaladsl.MqttFlow
      .atLeastOnce(settings, subscriptions, bufferSize, defaultQos)
      .map(MqttMessageWithAck.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a flow to send messages to MQTT , send acknowledge AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atLeastOnceWithAck(
      settings: MqttConnectionSettings,
      subscriptions: MqttSubscriptions,
      bufferSize: Int,
      defaultQos: MqttQoS
  ): Flow[MqttMessageWithAck, MqttMessageWithAck, CompletionStage[Done]] =
    scaladsl.MqttFlow
      .atLeastOnceWithAckForJava(settings, subscriptions, bufferSize, defaultQos)
      .map(MqttMessageWithAck.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
}
