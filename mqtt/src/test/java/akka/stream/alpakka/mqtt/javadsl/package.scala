/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.mqtt.scaladsl.{MqttCommittableMessage => ScalaMqttCommittableMessage}

import scala.compat.java8.FutureConverters

/**
  * This implicit classes allow to convert the Committable and CommittableMessage between scaladsl and javadsl.
  */
package object javadsl {
  import FutureConverters._

  private[javadsl] implicit class RichMqttCommittableMessage(cm: ScalaMqttCommittableMessage) {
    def asJava: MqttCommittableMessage = new MqttCommittableMessage {
      override val message: MqttMessage = cm.message
      override def messageArrivedComplete(): CompletionStage[Done] = cm.messageArrivedComplete().toJava
    }
  }
}