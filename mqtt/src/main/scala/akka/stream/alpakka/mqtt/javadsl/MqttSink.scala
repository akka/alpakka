/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.{MqttConnectionSettings, MqttMessage, MqttQoS, MqttSourceSettings}
import akka.stream.javadsl.Sink

/**
 * Java API
 *
 * MQTT sink factory.
 */
object MqttSink {

  /**
   * Create a sink sending messages to MQTT.
   */
  def create(connectionSettings: MqttConnectionSettings, qos: MqttQoS): Sink[MqttMessage, CompletionStage[Done]] =
    MqttFlow
      .atMostOnce(MqttSourceSettings(connectionSettings), bufferSize = 0, qos)
      .to(Sink.ignore[MqttMessage])
}
