/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.impl.MqttFlowStage
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT flow factory.
 */
object MqttFlow {

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages (without a commit handle).
   *
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  @deprecated("use atMostOnce instead", "0.21")
  def apply(sourceSettings: MqttSourceSettings,
            bufferSize: Int,
            defaultQos: MqttQoS): Flow[MqttMessage, MqttMessage, Future[Done]] =
    atMostOnce(sourceSettings, bufferSize, defaultQos)

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages (without a commit handle).
   *
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atMostOnce(sourceSettings: MqttSourceSettings,
                 bufferSize: Int,
                 defaultQos: MqttQoS): Flow[MqttMessage, MqttMessage, Future[Done]] =
    Flow
      .fromGraph(new MqttFlowStage(sourceSettings, bufferSize, defaultQos))
      .map(_.message)

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   *
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atLeastOnce(sourceSettings: MqttSourceSettings,
                  bufferSize: Int,
                  defaultQos: MqttQoS): Flow[MqttMessage, MqttCommittableMessage, Future[Done]] =
    Flow.fromGraph(new MqttFlowStage(sourceSettings, bufferSize, defaultQos, manualAcks = true))
}
