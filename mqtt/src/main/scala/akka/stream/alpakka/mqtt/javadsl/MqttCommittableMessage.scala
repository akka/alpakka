/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.MqttMessage

trait MqttCommittableMessage {
  val message: MqttMessage
  def messageArrivedComplete(): CompletionStage[Done]
}
