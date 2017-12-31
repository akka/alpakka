/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.{Keep, Sink}

import scala.concurrent.Future

object MqttSink {

  /**
   * Scala API: create an [[MqttSink]] for a provided QoS.
   */
  def apply(connectionSettings: MqttConnectionSettings, qos: MqttQoS): Sink[MqttMessage, Future[Done]] =
    MqttFlow(MqttSourceSettings(connectionSettings, Map.empty), 0, qos)
      .toMat(Sink.ignore)(Keep.right)

}
