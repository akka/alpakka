/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.{Keep, Source}

import scala.concurrent.Future

/**
 * Scala API
 *
 * MQTT source factory.
 */
object MqttSource {

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   */
  def atMostOnce(settings: MqttConnectionSettings,
                 subscriptions: MqttSubscriptions,
                 bufferSize: Int): Source[MqttMessage, Future[Done]] =
    Source.maybe
      .viaMat(
        MqttFlow.atMostOnce(settings, subscriptions, bufferSize, defaultQos = MqttQoS.AtLeastOnce)
      )(Keep.right)

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   */
  def atLeastOnce(settings: MqttConnectionSettings,
                  subscriptions: MqttSubscriptions,
                  bufferSize: Int): Source[MqttMessageWithAck, Future[Done]] =
    Source.maybe.viaMat(
      MqttFlow.atLeastOnce(settings, subscriptions, bufferSize, defaultQos = MqttQoS.AtLeastOnce)
    )(Keep.right)

}
