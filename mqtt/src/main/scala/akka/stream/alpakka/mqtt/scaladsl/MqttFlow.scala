/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.impl.{MqttFlowStage, MqttFlowStageWithAck}
import akka.stream.scaladsl.{Flow, Keep}

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
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atMostOnce(connectionSettings: MqttConnectionSettings,
                 subscriptions: MqttSubscriptions,
                 bufferSize: Int,
                 defaultQos: MqttQoS
  ): Flow[MqttMessage, MqttMessage, Future[Done]] =
    Flow
      .fromGraph(
        new MqttFlowStage(connectionSettings, subscriptions.subscriptions, bufferSize, defaultQos)
      )
      .map(_.message)

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atLeastOnce(connectionSettings: MqttConnectionSettings,
                  subscriptions: MqttSubscriptions,
                  bufferSize: Int,
                  defaultQos: MqttQoS
  ): Flow[MqttMessage, MqttMessageWithAck, Future[Done]] =
    Flow.fromGraph(
      new MqttFlowStage(connectionSettings, subscriptions.subscriptions, bufferSize, defaultQos, manualAcks = true)
    )

  /**
   * Create a flow to send messages to MQTT AND subscribe to MQTT messages with a commit handle to acknowledge message reception.
   * The acknowledge are fired in both messages
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def atLeastOnceWithAck(connectionSettings: MqttConnectionSettings,
                         subscriptions: MqttSubscriptions,
                         bufferSize: Int,
                         defaultQos: MqttQoS
  ): Flow[MqttMessageWithAck, MqttMessageWithAck, Future[Done]] =
    Flow.fromGraph(
      new MqttFlowStageWithAck(connectionSettings,
                               subscriptions.subscriptions,
                               bufferSize,
                               defaultQos,
                               manualAcks = true
      )
    )

  def atLeastOnceWithAckForJava(
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscriptions,
      bufferSize: Int,
      defaultQos: MqttQoS
  ): Flow[javadsl.MqttMessageWithAck, MqttMessageWithAck, Future[Done]] =
    Flow
      .fromFunction(MqttMessageWithAck.fromJava)
      .viaMat(atLeastOnceWithAck(connectionSettings, subscriptions, bufferSize, defaultQos))(Keep.right)

}
