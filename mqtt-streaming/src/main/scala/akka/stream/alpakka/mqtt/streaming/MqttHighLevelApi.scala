/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Tcp}
import akka.util.ByteString
import akka.util.JavaDurationConverters._

import scala.concurrent.duration._

@ApiMayChange
trait MqttTransportSettings {
  def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _]
}

@ApiMayChange
final class MqttTcpTransportSettings private (
    val host: String,
    val port: Int
) extends MqttTransportSettings {

  def withHost(value: String): MqttTcpTransportSettings = copy(host = value)
  def withPort(value: Int): MqttTcpTransportSettings = copy(port = value)

  private def copy(
      host: String = host,
      port: Int = port
  ): MqttTcpTransportSettings = new MqttTcpTransportSettings(
    host = host,
    port = port
  )

  override def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _] =
    Tcp(system).outgoingConnection(host, port)

  override def toString =
    "MqttTcpTransportSettings(" +
    s"host=$host," +
    s"port=$port" +
    ")"
}

@ApiMayChange
object MqttTcpTransportSettings {
  def apply(host: String): MqttTcpTransportSettings = apply(host, 1883)
  def apply(host: String, port: Int): MqttTcpTransportSettings = new MqttTcpTransportSettings(host, port)

  /** Java API */
  def create(host: String): MqttTcpTransportSettings = apply(host)
  def create(host: String, port: Int): MqttTcpTransportSettings = apply(host, port)
}

@ApiMayChange
final class MqttConnectionSettings private (
    val clientId: String,
    val connectFlags: ConnectFlags,
    val bufferSize: Int
) {

  def withClientId(value: String): MqttConnectionSettings = copy(clientId = value)
  def withConnectFlags(value: ConnectFlags): MqttConnectionSettings = copy(connectFlags = value)
  def withBufferSize(value: Int): MqttConnectionSettings = copy(bufferSize = value)

  private def copy(
      clientId: String = clientId,
      connectFlags: ConnectFlags = connectFlags,
      bufferSize: Int = bufferSize
  ): MqttConnectionSettings = new MqttConnectionSettings(
    clientId = clientId,
    connectFlags = connectFlags,
    bufferSize = bufferSize
  )

  def createControlPacket: Connect = Connect(clientId, connectFlags)

  override def toString =
    "MqttConnectionSettings(" +
    s"clientId=$clientId," +
    s"connectFlags=$connectFlags," +
    s"bufferSize=$bufferSize" +
    ")"
}

@ApiMayChange
object MqttConnectionSettings {

  /** Scala API */
  def apply(clientId: String): MqttConnectionSettings =
    new MqttConnectionSettings(clientId, connectFlags = ConnectFlags.CleanSession, bufferSize = 10)

  /** Java API */
  def create(clientId: String): MqttConnectionSettings = apply(clientId)
}

@ApiMayChange
final class MqttRestartSettings private (
    val minBackoff: scala.concurrent.duration.FiniteDuration,
    val maxBackoff: scala.concurrent.duration.FiniteDuration,
    val randomFactor: Double,
    val maxRestarts: Int
) {

  /** Scala API */
  def withMinBackoff(value: scala.concurrent.duration.FiniteDuration): MqttRestartSettings = copy(minBackoff = value)

  /** Java API */
  def withMinBackoff(value: java.time.Duration): MqttRestartSettings = copy(minBackoff = value.asScala)

  /** Scala API */
  def withMaxBackoff(value: scala.concurrent.duration.FiniteDuration): MqttRestartSettings = copy(maxBackoff = value)

  /** Java API */
  def withMaxBackoff(value: java.time.Duration): MqttRestartSettings = copy(maxBackoff = value.asScala)
  def withRandomFactor(value: Double): MqttRestartSettings = copy(randomFactor = value)
  def withMaxRestarts(value: Int): MqttRestartSettings = copy(maxRestarts = value)

  private def copy(
      minBackoff: scala.concurrent.duration.FiniteDuration = minBackoff,
      maxBackoff: scala.concurrent.duration.FiniteDuration = maxBackoff,
      randomFactor: Double = randomFactor,
      maxRestarts: Int = maxRestarts
  ): MqttRestartSettings = new MqttRestartSettings(
    minBackoff = minBackoff,
    maxBackoff = maxBackoff,
    randomFactor = randomFactor,
    maxRestarts = maxRestarts
  )

  override def toString =
    "MqttRestartSettings(" +
    s"minBackoff=${minBackoff.toCoarsest}," +
    s"maxBackoff=${maxBackoff.toCoarsest}," +
    s"randomFactor=$randomFactor," +
    s"maxRestarts=$maxRestarts" +
    ")"
}

@ApiMayChange
object MqttRestartSettings {

  val Defaults = new MqttRestartSettings(
    minBackoff = 2.seconds,
    maxBackoff = 2.minutes,
    randomFactor = 0.42d,
    maxRestarts = -1
  )

  /** Scala API */
  def apply(): MqttRestartSettings = Defaults

  /** Java API */
  def create(): MqttRestartSettings = Defaults

}
