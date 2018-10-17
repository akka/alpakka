/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.time.Duration
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

object MqttSessionSettings {

  /**
   * Factory method for Scala.
   */
  def apply(maxPacketSize: Int = 4096,
            maxClientConnectionStashSize: Int = 100,
            actorMqttSessionTimeout: FiniteDuration = 3.seconds,
            commandParallelism: Int = 10,
            eventParallelism: Int = 10,
            receiveConnAckTimeout: FiniteDuration = 30.seconds,
            receivePubAckRecTimeout: FiniteDuration = 30.seconds,
            receivePubCompTimeout: FiniteDuration = 30.seconds,
            receivePubRelTimeout: FiniteDuration = 30.seconds,
            receiveSubAckTimeout: FiniteDuration = 30.seconds,
            receiveUnsubAckTimeout: FiniteDuration = 30.seconds,
            serverSendBufferSize: Int = 100): MqttSessionSettings =
    new MqttSessionSettings(
      maxPacketSize,
      maxClientConnectionStashSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout,
      serverSendBufferSize
    )

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): MqttSessionSettings =
    apply()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(maxPacketSize: Int,
             maxClientConnectionStashSize: Int,
             actorMqttSessionTimeout: Duration,
             commandParallelism: Int,
             eventParallelism: Int,
             receiveConnAckTimeout: Duration,
             receivePubAckRecTimeout: Duration,
             receivePubCompTimeout: Duration,
             receivePubRelTimeout: Duration,
             receiveSubAckTimeout: Duration,
             receiveUnsubAckTimeout: Duration,
             serverSendBufferSize: Int): MqttSessionSettings =
    MqttSessionSettings(
      maxPacketSize,
      maxClientConnectionStashSize,
      FiniteDuration(actorMqttSessionTimeout.toMillis, TimeUnit.MILLISECONDS),
      commandParallelism,
      eventParallelism,
      FiniteDuration(receiveConnAckTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubAckRecTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubCompTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubRelTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receiveSubAckTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receiveUnsubAckTimeout.toMillis, TimeUnit.MILLISECONDS),
      serverSendBufferSize
    )
}

final class MqttSessionSettings private (val maxPacketSize: Int,
                                         val maxClientConnectionStashSize: Int,
                                         val actorMqttSessionTimeout: FiniteDuration,
                                         val commandParallelism: Int,
                                         val eventParallelism: Int,
                                         val receiveConnAckTimeout: FiniteDuration,
                                         val receivePubAckRecTimeout: FiniteDuration,
                                         val receivePubCompTimeout: FiniteDuration,
                                         val receivePubRelTimeout: FiniteDuration,
                                         val receiveSubAckTimeout: FiniteDuration,
                                         val receiveUnsubAckTimeout: FiniteDuration,
                                         val serverSendBufferSize: Int) {

  require(
    commandParallelism >= 2,
    s"commandParallelism of $commandParallelism must be greater than or equal to 2 to support connection replies such as pinging"
  )
  require(maxPacketSize >= 0 && maxPacketSize <= 0xffff,
          s"maxPacketSize of $maxPacketSize must be positive and less than ${0xffff}")

  def withMaxPacketSize(maxPacketSize: Int): MqttSessionSettings = copy(maxPacketSize = maxPacketSize)

  def withActorMqttSessionTimeout(actorMqttSessionTimeout: FiniteDuration): MqttSessionSettings =
    copy(actorMqttSessionTimeout = actorMqttSessionTimeout)

  def withCommandParallelism(commandParallelism: Int): MqttSessionSettings =
    copy(commandParallelism = commandParallelism)

  def withEventParallelism(eventParallelism: Int): MqttSessionSettings =
    copy(eventParallelism = eventParallelism)

  def withReceiveConnAckTimeout(receiveConnAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout)

  def withReceivePubAckRecTimeout(receivePubAckRecTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubAckRecTimeout = receivePubAckRecTimeout)

  def withReceivePubCompTimeout(receivePubCompTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubCompTimeout = receivePubCompTimeout)

  def withReceivePubRelTimeout(receivePubRelTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubRelTimeout = receivePubRelTimeout)

  def withReceiveSubAckTimeout(receiveSubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveSubAckTimeout = receiveSubAckTimeout)

  def withReceiveUnsubAckTimeout(receiveUnsubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveUnsubAckTimeout = receiveUnsubAckTimeout)

  def withMaxClientConnectionStashSize(maxClientConnectionStashSize: Int): MqttSessionSettings =
    copy(maxClientConnectionStashSize = maxClientConnectionStashSize)

  def withServerSendBufferSize(serverSendBufferSize: Int): MqttSessionSettings =
    copy(serverSendBufferSize = serverSendBufferSize)

  private def copy(maxPacketSize: Int = maxPacketSize,
                   maxClientConnectionStashSize: Int = maxClientConnectionStashSize,
                   actorMqttSessionTimeout: FiniteDuration = actorMqttSessionTimeout,
                   commandParallelism: Int = commandParallelism,
                   eventParallelism: Int = eventParallelism,
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
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout,
      serverSendBufferSize
    )

  override def toString: String =
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,maxClientConnectionStashSize=$maxClientConnectionStashSize,actorMqttSessionTimeout=$actorMqttSessionTimeout,commandParallelism=$commandParallelism,eventParallelism=$eventParallelism,receiveConnAckTimeout=$receiveConnAckTimeout,receivePubAckRecTimeout=$receivePubAckRecTimeout,receivePubCompTimeout=$receivePubCompTimeout,receivePubRelTimeout=$receivePubRelTimeout,receiveSubAckTimeout=$receiveSubAckTimeout,receiveUnsubAckTimeout=$receiveUnsubAckTimeout,serverSendBufferSize=$serverSendBufferSize)"
}
