/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Tcp}
import akka.util.ByteString

import scala.concurrent.duration._

@ApiMayChange
trait MqttTransportSettings {
  def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _]
}

final case class MqttTcpTransportSettings(val host: String = "localhost", val port: Int = 1883)
    extends MqttTransportSettings {

  def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _] =
    Tcp(system).outgoingConnection(host, port)

}

final case class MqttConnectionSettings(
    val clientId: String,
    val connectFlags: ConnectFlags = ConnectFlags.CleanSession,
    val bufferSize: Int = 10
) {
  def createControlPacket: Connect = Connect(clientId, connectFlags)
}

final case class MqttRestartSettings(
    val minBackoff: FiniteDuration = 2.seconds,
    val maxBackoff: FiniteDuration = 2.minutes,
    val randomFactor: Double = 0.42d,
    val maxRestarts: Int = -1
)
