/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

import java.time.Duration

import scala.concurrent.duration._

object MqttSessionSettings {

  /**
   * Factory method for Scala.
   */
  def apply(): MqttSessionSettings =
    new MqttSessionSettings()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): MqttSessionSettings =
    apply()
}

final class MqttSessionSettings private (val maxPacketSize: Int = 4096,
                                         val maxClientConnectionStashSize: Int = 100,
                                         val clientTerminationWatcherBufferSize: Int = 100,
                                         val actorMqttSessionTimeout: FiniteDuration = 3.seconds,
                                         val commandParallelism: Int = 10,
                                         val eventParallelism: Int = 10,
                                         val receiveConnectTimeout: FiniteDuration = 5.minutes,
                                         val receiveConnAckTimeout: FiniteDuration = 30.seconds,
                                         val receivePubAckRecTimeout: FiniteDuration = 30.seconds,
                                         val receivePubCompTimeout: FiniteDuration = 30.seconds,
                                         val receivePubRelTimeout: FiniteDuration = 30.seconds,
                                         val receiveSubAckTimeout: FiniteDuration = 30.seconds,
                                         val receiveUnsubAckTimeout: FiniteDuration = 30.seconds,
                                         val serverSendBufferSize: Int = 100) {

  require(
    commandParallelism >= 2,
    s"commandParallelism of $commandParallelism must be greater than or equal to 2 to support connection replies such as pinging"
  )
  require(maxPacketSize >= 0 && maxPacketSize <= 0xffff,
          s"maxPacketSize of $maxPacketSize must be positive and less than ${0xffff}")

  import akka.util.JavaDurationConverters._

  def withMaxPacketSize(maxPacketSize: Int): MqttSessionSettings = copy(maxPacketSize = maxPacketSize)

  def withActorMqttSessionTimeout(actorMqttSessionTimeout: FiniteDuration): MqttSessionSettings =
    copy(actorMqttSessionTimeout = actorMqttSessionTimeout)

  def withActorMqttSessionTimeout(actorMqttSessionTimeout: Duration): MqttSessionSettings =
    copy(actorMqttSessionTimeout = actorMqttSessionTimeout.asScala)

  def withCommandParallelism(commandParallelism: Int): MqttSessionSettings =
    copy(commandParallelism = commandParallelism)

  def withEventParallelism(eventParallelism: Int): MqttSessionSettings =
    copy(eventParallelism = eventParallelism)

  def withReceiveConnectTimeout(receiveConnectTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnectTimeout = receiveConnectTimeout)

  def withReceiveConnectTimeout(receiveConnectTimeout: Duration): MqttSessionSettings =
    copy(receiveConnectTimeout = receiveConnectTimeout.asScala)

  def withReceiveConnAckTimeout(receiveConnAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout)

  def withReceiveConnAckTimeout(receiveConnAckTimeout: Duration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout.asScala)

  def withReceivePubAckRecTimeout(receivePubAckRecTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubAckRecTimeout = receivePubAckRecTimeout)

  def withReceivePubAckRecTimeout(receivePubAckRecTimeout: Duration): MqttSessionSettings =
    copy(receivePubAckRecTimeout = receivePubAckRecTimeout.asScala)

  def withReceivePubCompTimeout(receivePubCompTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubCompTimeout = receivePubCompTimeout)

  def withReceivePubCompTimeout(receivePubCompTimeout: Duration): MqttSessionSettings =
    copy(receivePubCompTimeout = receivePubCompTimeout.asScala)

  def withReceivePubRelTimeout(receivePubRelTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubRelTimeout = receivePubRelTimeout)

  def withReceivePubRelTimeout(receivePubRelTimeout: Duration): MqttSessionSettings =
    copy(receivePubRelTimeout = receivePubRelTimeout.asScala)

  def withReceiveSubAckTimeout(receiveSubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveSubAckTimeout = receiveSubAckTimeout)

  def withReceiveSubAckTimeout(receiveSubAckTimeout: Duration): MqttSessionSettings =
    copy(receiveSubAckTimeout = receiveSubAckTimeout.asScala)

  def withReceiveUnsubAckTimeout(receiveUnsubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveUnsubAckTimeout = receiveUnsubAckTimeout)

  def withReceiveUnsubAckTimeout(receiveUnsubAckTimeout: Duration): MqttSessionSettings =
    copy(receiveUnsubAckTimeout = receiveUnsubAckTimeout.asScala)

  def withMaxClientConnectionStashSize(maxClientConnectionStashSize: Int): MqttSessionSettings =
    copy(maxClientConnectionStashSize = maxClientConnectionStashSize)

  def withClientTerminationWatcherBufferSize(clientTerminationWatcherBufferSize: Int): MqttSessionSettings =
    copy(clientTerminationWatcherBufferSize = clientTerminationWatcherBufferSize)

  def withServerSendBufferSize(serverSendBufferSize: Int): MqttSessionSettings =
    copy(serverSendBufferSize = serverSendBufferSize)

  private def copy(maxPacketSize: Int = maxPacketSize,
                   maxClientConnectionStashSize: Int = maxClientConnectionStashSize,
                   clientTerminationWatcherBufferSize: Int = clientTerminationWatcherBufferSize,
                   actorMqttSessionTimeout: FiniteDuration = actorMqttSessionTimeout,
                   commandParallelism: Int = commandParallelism,
                   eventParallelism: Int = eventParallelism,
                   receiveConnectTimeout: FiniteDuration = receiveConnectTimeout,
                   receiveConnAckTimeout: FiniteDuration = receiveConnAckTimeout,
                   receivePubAckRecTimeout: FiniteDuration = receivePubAckRecTimeout,
                   receivePubCompTimeout: FiniteDuration = receivePubCompTimeout,
                   receivePubRelTimeout: FiniteDuration = receivePubRelTimeout,
                   receiveSubAckTimeout: FiniteDuration = receiveSubAckTimeout,
                   receiveUnsubAckTimeout: FiniteDuration = receiveUnsubAckTimeout,
                   serverSendBufferSize: Int = serverSendBufferSize) =
    new MqttSessionSettings(
      maxPacketSize,
      maxClientConnectionStashSize,
      clientTerminationWatcherBufferSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnectTimeout,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout,
      serverSendBufferSize
    )

  override def toString: String =
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,maxClientConnectionStashSize=$maxClientConnectionStashSize,clientTerminationWatcherBufferSize=$clientTerminationWatcherBufferSize,actorMqttSessionTimeout=$actorMqttSessionTimeout,commandParallelism=$commandParallelism,eventParallelism=$eventParallelism,receiveConnectTimeout=$receiveConnectTimeout,receiveConnAckTimeout=$receiveConnAckTimeout,receivePubAckRecTimeout=$receivePubAckRecTimeout,receivePubCompTimeout=$receivePubCompTimeout,receivePubRelTimeout=$receivePubRelTimeout,receiveSubAckTimeout=$receiveSubAckTimeout,receiveUnsubAckTimeout=$receiveUnsubAckTimeout,serverSendBufferSize=$serverSendBufferSize)"
}
