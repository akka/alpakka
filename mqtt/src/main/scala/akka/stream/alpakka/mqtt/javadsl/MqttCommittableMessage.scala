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
 *
 * MQTT Message and a handle to commit message reception to MQTT.
 */
sealed trait MqttCommittableMessage {

  /**
   * The message received from MQTT.
   */
  val message: MqttMessage

  /**
   * @deprecated use commit instead, since 0.21
   */
  @deprecated("use commit instead", "0.21")
  @java.lang.Deprecated
  def messageArrivedComplete(): CompletionStage[Done] = commit()

  /**
   * Signals `messageArrivedComplete` to MQTT.
   *
   * @return completion indicating, if the commit reached MQTT
   */
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
