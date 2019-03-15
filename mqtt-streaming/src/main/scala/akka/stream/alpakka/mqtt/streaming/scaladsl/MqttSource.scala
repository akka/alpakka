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

trait MqttAckHandle {

  def ack(): Future[Done]
}

final class MqttAckHandleScala @InternalApi private[scaladsl] (sendAck: () => Future[Done]) extends MqttAckHandle {

  def ack(): Future[Done] = sendAck.apply()

}

@ApiMayChange
object MqttSource {

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

  private def createOut(publish: Publish, ackHandle: () => Future[Done]): (Publish, MqttAckHandle) =
    (publish, new MqttAckHandleScala(ackHandle))

}
