/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.{ApiMayChange, InternalApi}
import akka.dispatch.ExecutionContexts
import akka.japi.Pair
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.impl.HighLevelMqttSource
import akka.stream.scaladsl.Source

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Java API:
 * Handle to send acknowledge for received Publish messages.
 */
@ApiMayChange
trait MqttAckHandle {

  /** Acknowledge received data. */
  def ack(): CompletionStage[Done]
}

/**
 * Internal API
 */
@InternalApi
final class MqttAckHandleJava private[javadsl] (sendAck: () => Future[Done]) extends MqttAckHandle {

  def ack(): CompletionStage[Done] = sendAck.apply().toJava

}

/**
 * Java API
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
  ): Source[Publish, CompletionStage[java.util.List[Pair[String, ControlPacketFlags]]]] =
    HighLevelMqttSource
      .atMostOnce(
        mqttClientSession.underlying,
        transportSettings,
        restartSettings,
        connectionSettings,
        subscriptions
      )
      .mapMaterializedValue(matValueToJava)

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
      subscriptions: MqttSubscribe
  ): Source[Pair[Publish, MqttAckHandle], CompletionStage[java.util.List[Pair[String, ControlPacketFlags]]]] =
    HighLevelMqttSource
      .atLeastOnce(
        mqttClientSession.underlying,
        transportSettings,
        restartSettings,
        connectionSettings,
        subscriptions,
        createOut
      )
      .mapMaterializedValue(matValueToJava)

  private def matValueToJava(f: Future[immutable.Seq[(String, ControlPacketFlags)]]) =
    f.map {
      _.map {
        case (t, f) => Pair.create(t, f)
      }.asJava
    }(ExecutionContexts.sameThreadExecutionContext).toJava

  private def createOut(publish: Publish, ackHandle: () => Future[Done]): Pair[Publish, MqttAckHandle] =
    Pair.create(publish, new MqttAckHandleJava(ackHandle))

}
