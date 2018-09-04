/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT sink factory.
 */
object MqttSink {

  /**
   * Create a sink sending messages to MQTT.
   */
  def apply(connectionSettings: MqttConnectionSettings, qos: MqttQoS): Sink[MqttMessage, Future[Done]] =
    MqttFlow
      .atMostOnce(MqttSourceSettings(connectionSettings, Map.empty), 0, qos)
      .to(Sink.ignore)

}
