/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.mqtt.MqttMessage
import akka.stream.alpakka.mqtt.scaladsl

import scala.compat.java8.FutureConverters._

/**
 * Java API
 * Message and handle to commit message arrival to MQTT.
 */
trait MqttCommittableMessage {
  val message: MqttMessage

  /**
   * @deprecated use commit instead, since 0.21
   */
  @deprecated("use commit instead", "0.21")
  @Deprecated
  def messageArrivedComplete(): CompletionStage[Done] = commit()
  def commit(): CompletionStage[Done]
}

/**
 * INTERNAL API
 */
@InternalApi
private[javadsl] object MqttCommittableMessage {
  def toJava(cm: scaladsl.MqttCommittableMessage): MqttCommittableMessage = new MqttCommittableMessage {
    override val message: MqttMessage = cm.message
    override def commit(): CompletionStage[Done] = cm.commit().toJava
  }
}
