/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.Flow
import scala.concurrent.Future

object MqttFlow {
  def apply(sourceSettings: MqttSourceSettings,
            bufferSize: Int,
            qos: MqttQoS): Flow[MqttMessage, MqttMessage, Future[Done]] =
    Flow.fromGraph(new MqttFlowStage(sourceSettings, bufferSize, qos))
}
