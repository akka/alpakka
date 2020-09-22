/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Tcp}
import akka.util.ByteString
import akka.util.JavaDurationConverters._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

@ApiMayChange
trait MqttTransportSettings {
  def connectionFlow()(implicit system: ClassicActorSystemProvider): Flow[ByteString, ByteString, _]
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

  override def connectionFlow()(implicit system: ClassicActorSystemProvider): Flow[ByteString, ByteString, _] =
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

/**
 * Create a `Connect` packet for the high-level APIs.
 * [[MqttConnectionSettings]] offers the most applicable implementation.
 */
@ApiMayChange
trait MqttConnectPacket {

  /** Packet sent to connect to MQTT broker. */
  def controlPacket: Connect

  /** Controls how many `Commands` are accepted internally before back-pressure applies, affects acknowledging. */
  def bufferSize: Int
}

@ApiMayChange
final class MqttConnectionSettings private (
    val clientId: String,
    val connectFlags: ConnectFlags,
    override val bufferSize: Int
) extends MqttConnectPacket {

  def withClientId(value: String): MqttConnectionSettings = copy(clientId = value)
  def withConnectFlags(value: ConnectFlags): MqttConnectionSettings = copy(connectFlags = value)

  /** Controls how many `Commands` are accepted internally before back-pressure applies, affects acknowledging. */
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

  override def controlPacket: Connect = Connect(clientId, connectFlags)

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

/**
 * Configuration for connection restarting in high-level API.
 * See [[akka.stream.scaladsl.RestartFlow]] for settings.
 */
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

/**
 * Create a `Subscribe` packet for high-level APIs.
 * [[MqttSubscriptions]] is the most applicable implementation.
 */
@ApiMayChange
trait MqttSubscribe {
  def controlPacket: Subscribe
}

/**
 * The mapping of topics to subscribe to and their control flags (including Quality of Service) per topic.
 */
@ApiMayChange
final class MqttSubscriptions private (
    val subscriptions: Map[String, ControlPacketFlags]
) extends MqttSubscribe {

  /** Scala API */
  def withSubscriptions(subscriptions: Map[String, ControlPacketFlags]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions)

  /** Java API */
  def withSubscriptions(subscriptions: java.util.Map[String, ControlPacketFlags]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.asScala.toMap)

  /** Add this subscription to the map of subscriptions configured already. */
  def addSubscription(topic: String, qos: ControlPacketFlags): MqttSubscriptions =
    new MqttSubscriptions(this.subscriptions.updated(topic, qos))

  def addAtMostOnce(topic: String): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.updated(topic, ControlPacketFlags.QoSAtMostOnceDelivery))

  def addAtLeastOnce(topic: String): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.updated(topic, ControlPacketFlags.QoSAtLeastOnceDelivery))

  def controlPacket: Subscribe = Subscribe(subscriptions.toList)

}

/**
 * The mapping of topics to subscribe to and their control flags (including Quality of Service) per topic.
 */
@ApiMayChange
object MqttSubscriptions {

  /** Scala API */
  def apply(subscriptions: Map[String, ControlPacketFlags]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions)

  /** Scala API */
  def apply(topic: String, qos: ControlPacketFlags): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> qos))

  /** Scala API */
  def apply(subscription: (String, ControlPacketFlags)): MqttSubscriptions =
    new MqttSubscriptions(Map(subscription))

  /** Java API */
  def create(subscriptions: java.util.List[akka.japi.Pair[String, ControlPacketFlags]]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.asScala.map(_.toScala).toMap)

  /** Java API */
  def create(topic: String, qos: ControlPacketFlags): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> qos))

  def atMostOnce(topic: String): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> ControlPacketFlags.QoSAtMostOnceDelivery))

  def atLeastOnce(topic: String): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery))

}
