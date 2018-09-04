/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
   * Create a source subscribing to MQTT messages.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  @deprecated("use atMostOnce instead", "0.15")
  def apply(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, Future[Done]] =
    atMostOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  @deprecated("use atMostOnce with MqttConnectionSettings and MqttSubscriptions instead", "0.21")
  def atMostOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, Future[Done]] =
    atMostOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
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
   */
  @deprecated("use atLeastOnce with MqttConnectionSettings and MqttSubscriptions instead", "0.21")
  def atLeastOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttCommittableMessage, Future[Done]] =
    atLeastOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  def atLeastOnce(settings: MqttConnectionSettings,
                  subscriptions: MqttSubscriptions,
                  bufferSize: Int): Source[MqttCommittableMessage, Future[Done]] =
    Source.maybe.viaMat(
      MqttFlow.atLeastOnce(settings, subscriptions, bufferSize, defaultQos = MqttQoS.AtLeastOnce)
    )(Keep.right)

}
