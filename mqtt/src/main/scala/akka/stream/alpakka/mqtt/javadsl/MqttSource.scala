/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.{scaladsl, MqttMessage, MqttSourceSettings}
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
   */
  def atMostOnce(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, CompletionStage[Done]] =
    scaladsl.MqttSource
      .atMostOnce(settings, bufferSize)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a source subscribing to MQTT messages with a commit handle to acknowledge message reception.
   *
   * The materialized value completes on successful connection to the MQTT broker.
   */
  def atLeastOnce(settings: MqttSourceSettings,
                  bufferSize: Int): Source[MqttCommittableMessage, CompletionStage[Done]] =
    scaladsl.MqttSource
      .atLeastOnce(settings, bufferSize)
      .map(MqttCommittableMessage.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
}
