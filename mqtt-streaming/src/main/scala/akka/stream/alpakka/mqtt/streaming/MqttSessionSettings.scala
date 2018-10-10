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
            maxConnectStashSize: Int = 100,
            actorMqttSessionTimeout: FiniteDuration = 3.seconds,
            commandParallelism: Int = 10,
            eventParallelism: Int = 10,
            receiveConnAckTimeout: FiniteDuration = 30.seconds,
            receivePubAckRecTimeout: FiniteDuration = 30.seconds,
            receivePubCompTimeout: FiniteDuration = 30.seconds,
            receivePubRelTimeout: FiniteDuration = 30.seconds,
            receiveSubAckTimeout: FiniteDuration = 30.seconds,
            receiveUnsubAckTimeout: FiniteDuration = 30.seconds): MqttSessionSettings =
    new MqttSessionSettings(
      maxPacketSize,
      maxConnectStashSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout
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
             maxConnectStashSize: Int,
             actorMqttSessionTimeout: Duration,
             commandParallelism: Int,
             eventParallelism: Int,
             receiveConnAckTimeout: Duration,
             receivePubAckRecTimeout: Duration,
             receivePubCompTimeout: Duration,
             receivePubRelTimeout: Duration,
             receiveSubAckTimeout: Duration): MqttSessionSettings =
    MqttSessionSettings(
      maxPacketSize,
      maxConnectStashSize,
      FiniteDuration(actorMqttSessionTimeout.toMillis, TimeUnit.MILLISECONDS),
      commandParallelism,
      eventParallelism,
      FiniteDuration(receiveConnAckTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubAckRecTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubCompTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receivePubRelTimeout.toMillis, TimeUnit.MILLISECONDS),
      FiniteDuration(receiveSubAckTimeout.toMillis, TimeUnit.MILLISECONDS)
    )
}

final class MqttSessionSettings private (val maxPacketSize: Int,
                                         val maxConnectStashSize: Int,
                                         val actorMqttSessionTimeout: FiniteDuration,
                                         val commandParallelism: Int,
                                         val eventParallelism: Int,
                                         val receiveConnAckTimeout: FiniteDuration,
                                         val receivePubAckRecTimeout: FiniteDuration,
                                         val receivePubCompTimeout: FiniteDuration,
                                         val receivePubRelTimeout: FiniteDuration,
                                         val receiveSubAckTimeout: FiniteDuration,
                                         val receiveUnsubAckTimeout: FiniteDuration) {

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

  def withMaxConnectStashSize(maxConnectStashSize: Int): MqttSessionSettings =
    copy(maxConnectStashSize = maxConnectStashSize)

  private def copy(maxPacketSize: Int = maxPacketSize,
                   maxConnectStashSize: Int = maxConnectStashSize,
                   actorMqttSessionTimeout: FiniteDuration = actorMqttSessionTimeout,
                   commandParallelism: Int = commandParallelism,
                   eventParallelism: Int = eventParallelism,
                   receiveConnAckTimeout: FiniteDuration = receiveConnAckTimeout,
                   receivePubAckRecTimeout: FiniteDuration = receivePubAckRecTimeout,
                   receivePubCompTimeout: FiniteDuration = receivePubCompTimeout,
                   receivePubRelTimeout: FiniteDuration = receivePubRelTimeout,
                   receiveSubAckTimeout: FiniteDuration = receiveSubAckTimeout,
                   receiveUnsubAckTimeout: FiniteDuration = receiveUnsubAckTimeout) =
    new MqttSessionSettings(
      maxPacketSize,
      maxConnectStashSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout
    )

  override def toString: String =
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,maxConnectStashSize=$maxConnectStashSize,actorMqttSessionTimeout=$actorMqttSessionTimeout,commandParallelism=$commandParallelism,eventParallelism=$eventParallelism,receiveConnAckTimeout=$receiveConnAckTimeout,receivePubAckRecTimeout=$receivePubAckRecTimeout,receivePubCompTimeout=$receivePubCompTimeout,receivePubRelTimeout=$receivePubRelTimeout,receiveSubAckTimeout=$receiveSubAckTimeout,receiveUnsubAckTimeout=$receiveUnsubAckTimeout)"
}
