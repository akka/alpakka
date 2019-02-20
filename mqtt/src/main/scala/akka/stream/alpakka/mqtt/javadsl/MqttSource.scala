/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @deprecated use atMostOnce with MqttConnectionSettings and MqttSubscriptions instead
   */
  @deprecated("use atMostOnce with MqttConnectionSettings and MqttSubscriptions instead", "1.0-M1")
  @java.lang.Deprecated
  def atMostOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, CompletionStage[Done]] =
    atMostOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages (without a commit handle).
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
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
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   * @deprecated use atLeastOnce with MqttConnectionSettings and MqttSubscriptions instead
   */
  @deprecated("use atLeastOnce with MqttConnectionSettings and MqttSubscriptions instead", "1.0-M1")
  @java.lang.Deprecated
  def atLeastOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessageWithAck, CompletionStage[Done]] =
    atLeastOnce(settings.connectionSettings, MqttSubscriptions(settings.subscriptions), bufferSize)

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   *
   * @param bufferSize max number of messages read from MQTT before back-pressure applies
   */
  def atLeastOnce(settings: MqttConnectionSettings,
                  subscriptions: MqttSubscriptions,
                  bufferSize: Int): Source[MqttMessageWithAck, CompletionStage[Done]] =
    scaladsl.MqttSource
      .atLeastOnce(settings, subscriptions, bufferSize)
      .map(MqttMessageWithAck.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
}
