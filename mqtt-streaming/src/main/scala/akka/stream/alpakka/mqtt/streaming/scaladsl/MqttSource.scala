/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.scaladsl

import akka.Done
import akka.annotation.{ApiMayChange, InternalApi}
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.impl.HighLevelMqttSource
import akka.stream.scaladsl.Source

import scala.collection.immutable
import scala.concurrent.Future

@ApiMayChange
trait MqttAckHandle {

  /** Acknowledge received data. */
  def ack(): Future[Done]
}

/**
 * Scala API
 */
@ApiMayChange
object MqttSource {

  /**
   * High-level API to subscribe to MQTT topics with at-most-once semantics.
   */
  @ApiMayChange
  def atMostOnce(
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe
  ): Source[Publish, Future[immutable.Seq[(String, ControlPacketFlags)]]] =
    HighLevelMqttSource.atMostOnce(
      mqttClientSession,
      transportSettings,
      restartSettings,
      connectionSettings,
      subscriptions
    )

  /**
   * High-level API to subscribe to MQTT topics with at-least-once semantics.
   * The second value in the emitted pairs offers the `ack()` method to acknowledge received packages to MQTT.
   */
  @ApiMayChange
  def atLeastOnce(
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe,
  ): Source[(Publish, MqttAckHandle), Future[immutable.Seq[(String, ControlPacketFlags)]]] =
    HighLevelMqttSource.atLeastOnce(
      mqttClientSession,
      transportSettings,
      restartSettings,
      connectionSettings,
      subscriptions,
      createOut
    )

  /**
   * Internal API
   */
  @InternalApi
  private final class MqttAckHandleImpl(sendAck: () => Future[Done]) extends MqttAckHandle {

    def ack(): Future[Done] = sendAck.apply()

  }

  private def createOut(publish: Publish, ackHandle: () => Future[Done]): (Publish, MqttAckHandle) =
    (publish, new MqttAckHandleImpl(ackHandle))

}
