/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.javadsl.Source

import scala.compat.java8.FutureConverters._

/**
 * Java API
 *
 * MQTT source factory.
 */
object MqttSource {

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @deprecated use atMostOnce with MqttConnectionSettings and MqttSubscriptions instead
   */
  @deprecated("use atMostOnce with MqttConnectionSettings and MqttSubscriptions instead", "0.21")
  @java.lang.Deprecated
  def atMostOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, CompletionStage[Done]] =
    atMostOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  def atMostOnce(settings: MqttConnectionSettings,
                 subscriptions: MqttSubscriptions,
                 bufferSize: Int): Source[MqttMessage, CompletionStage[Done]] =
    scaladsl.MqttSource
      .atMostOnce(settings, subscriptions, bufferSize)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @deprecated use atLeastOnce with MqttConnectionSettings and MqttSubscriptions instead
   */
  @deprecated("use atLeastOnce with MqttConnectionSettings and MqttSubscriptions instead", "0.21")
  @java.lang.Deprecated
  def atLeastOnce(settings: MqttSourceSettings,
                  bufferSize: Int): Source[MqttCommittableMessage, CompletionStage[Done]] =
    atLeastOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  def atLeastOnce(settings: MqttConnectionSettings,
                  subscriptions: MqttSubscriptions,
                  bufferSize: Int): Source[MqttCommittableMessage, CompletionStage[Done]] =
    scaladsl.MqttSource
      .atLeastOnce(settings, subscriptions, bufferSize)
      .map(MqttCommittableMessage.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
}
