/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.{MqttMessage, MqttQoS, MqttSourceSettings}
import akka.stream.scaladsl.{Keep, Source}

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT source factory.
 */
object MqttSource {

  /**
   * Create a source subscribing to MQTT messages.
   */
  @deprecated("use atMostOnce instead", "0.15")
  def apply(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, Future[Done]] =
    atMostOnce(settings, bufferSize)

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   */
  def atMostOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, Future[Done]] =
    Source.maybe
      .viaMat(
        MqttFlow.atMostOnce(settings, bufferSize, qos = MqttQoS.AtLeastOnce)
      )(Keep.right)

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   */
  def atLeastOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttCommittableMessage, Future[Done]] =
    Source.maybe.viaMat(
      MqttFlow.atLeastOnce(settings, bufferSize, qos = MqttQoS.AtLeastOnce)
    )(Keep.right)

}
