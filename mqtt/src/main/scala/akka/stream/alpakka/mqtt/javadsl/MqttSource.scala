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
  def create(settings: MqttSourceSettings,
             bufferSize: Int): akka.stream.javadsl.Source[MqttMessage, CompletionStage[Done]] = {
    import scala.compat.java8.FutureConverters._
    akka.stream.alpakka.mqtt.scaladsl.MqttSource.apply(settings, bufferSize).mapMaterializedValue(_.toJava).asJava
  }
}
