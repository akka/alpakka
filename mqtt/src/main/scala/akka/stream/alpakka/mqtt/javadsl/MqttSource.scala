/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.{MqttMessage, MqttSourceSettings}

object MqttSource {

  /**
   * Java API: create an [[MqttSource]] with a provided bufferSize.
   */
  @deprecated("use atMostOnce instead", "0.15")
  def create(settings: MqttSourceSettings,
             bufferSize: Int): akka.stream.javadsl.Source[MqttMessage, CompletionStage[Done]] =
    atMostOnce(settings, bufferSize)

  def atMostOnce(settings: MqttSourceSettings,
                 bufferSize: Int): akka.stream.javadsl.Source[MqttMessage, CompletionStage[Done]] = {
    import scala.compat.java8.FutureConverters._
    akka.stream.alpakka.mqtt.scaladsl.MqttSource.atMostOnce(settings, bufferSize).mapMaterializedValue(_.toJava).asJava
  }

  def atLeastOnce(settings: MqttSourceSettings,
                  bufferSize: Int): akka.stream.javadsl.Source[MqttCommittableMessage, CompletionStage[Done]] = {
    import scala.compat.java8.FutureConverters._
    akka.stream.alpakka.mqtt.scaladsl.MqttSource
      .atLeastOnce(settings, bufferSize)
      .map(cm => cm.asJava)
      .mapMaterializedValue(_.toJava)
      .asJava
  }
}
