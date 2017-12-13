/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt.MqttMessage

import scala.concurrent.Future

trait MqttCommittableMessage {
  val message: MqttMessage
  def messageArrivedComplete(): Future[Done]
}
