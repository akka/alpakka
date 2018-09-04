/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage
import akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.{MqttMessage, MqttSourceSettings}

object MqttSource {

  def atMostOnce(settings: MqttSourceSettings,
                 bufferSize: Int): akka.stream.javadsl.Source[MqttMessage, CompletionStage[Done]] = {
    import scala.compat.java8.FutureConverters._
    scaladsl.MqttSource
      .atMostOnce(settings, bufferSize)
      .mapMaterializedValue(_.toJava)
      .asJava
  }

  def atLeastOnce(settings: MqttSourceSettings,
                  bufferSize: Int): akka.stream.javadsl.Source[MqttCommittableMessage, CompletionStage[Done]] = {
    import scala.compat.java8.FutureConverters._
    scaladsl.MqttSource
      .atLeastOnce(settings, bufferSize)
      .map(MqttCommittableMessage.toJava)
      .mapMaterializedValue(_.toJava)
      .asJava
  }
}
