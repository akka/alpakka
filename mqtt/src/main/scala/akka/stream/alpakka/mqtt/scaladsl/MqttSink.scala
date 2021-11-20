/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.{Keep, Sink}

import scala.concurrent.Future

/**
 * Scala API
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
  def apply(connectionSettings: MqttConnectionSettings, defaultQos: MqttQoS): Sink[MqttMessage, Future[Done]] =
    MqttFlow
      .atMostOnce(connectionSettings, MqttSubscriptions.empty, 0, defaultQos)
      .toMat(Sink.ignore)(Keep.right)

}
