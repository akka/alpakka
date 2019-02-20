/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.javadsl.{Keep, Sink}

/**
 * Java API
 *
 * MQTT sink factory.
 */
object MqttSink {

  /**
   * Create a sink sending messages to MQTT.
   *
   * The materialized value completes on stream completion.
   *
   * @param defaultQos Quality of service level applied for messages not specifying a message specific value
   */
  def create(connectionSettings: MqttConnectionSettings,
             defaultQos: MqttQoS): Sink[MqttMessage, CompletionStage[Done]] =
    MqttFlow
      .atMostOnce(connectionSettings, MqttSubscriptions.empty, bufferSize = 0, defaultQos)
      .toMat(Sink.ignore[MqttMessage], Keep.right[CompletionStage[Done], CompletionStage[Done]])
}
