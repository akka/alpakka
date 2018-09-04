/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.MqttMessage

import scala.concurrent.Future

/**
 * Scala API
 * Message and handle to commit message arrival to MQTT.
 */
trait MqttCommittableMessage {
  val message: MqttMessage
  @deprecated("use commit instead", "0.21")
  def messageArrivedComplete(): Future[Done] = commit()
  def commit(): Future[Done]
}
