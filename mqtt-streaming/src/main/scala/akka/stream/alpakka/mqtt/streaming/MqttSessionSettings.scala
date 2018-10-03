/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.time.Duration
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

object MqttSessionSettings {

  /**
   * Factory method for Scala.
   */
  def apply(maxPacketSize: Int,
            actorMqttSessionTimeout: FiniteDuration,
            commandParallelism: Int,
            eventParallelism: Int,
            receiveConnAckTimeout: FiniteDuration,
            receivePubAckRecTimeout: FiniteDuration,
            receivePubCompTimeout: FiniteDuration,
            receivePubRelTimeout: FiniteDuration,
            receiveSubAckTimeout: FiniteDuration): MqttSessionSettings =
    new MqttSessionSettings(
      maxPacketSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout
    )

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(maxPacketSize: Int,
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
                                         val actorMqttSessionTimeout: FiniteDuration,
                                         val commandParallelism: Int,
                                         val eventParallelism: Int,
                                         val receiveConnAckTimeout: FiniteDuration,
                                         val receivePubAckRecTimeout: FiniteDuration,
                                         val receivePubCompTimeout: FiniteDuration,
                                         val receivePubRelTimeout: FiniteDuration,
                                         val receiveSubAckTimeout: FiniteDuration) {

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

  private def copy(maxPacketSize: Int = maxPacketSize,
                   actorMqttSessionTimeout: FiniteDuration = actorMqttSessionTimeout,
                   commandParallelism: Int = commandParallelism,
                   eventParallelism: Int = eventParallelism,
                   receiveConnAckTimeout: FiniteDuration = receiveConnAckTimeout,
                   receivePubAckRecTimeout: FiniteDuration = receivePubAckRecTimeout,
                   receivePubCompTimeout: FiniteDuration = receivePubCompTimeout,
                   receivePubRelTimeout: FiniteDuration = receivePubRelTimeout,
                   receiveSubAckTimeout: FiniteDuration = receiveSubAckTimeout) =
    new MqttSessionSettings(
      maxPacketSize,
      actorMqttSessionTimeout,
      commandParallelism,
      eventParallelism,
      receiveConnAckTimeout,
      receivePubAckRecTimeout,
      receivePubCompTimeout,
      receivePubRelTimeout,
      receiveSubAckTimeout
    )

  override def toString: String =
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,actorMqttSessionTimeout=$actorMqttSessionTimeout,commandParallelism=$commandParallelism,eventParallelism=$eventParallelism,receiveConnAckTimeout=$receiveConnAckTimeout,receivePubAckRecTimeout=$receivePubAckRecTimeout,receivePubCompTimeout=$receivePubCompTimeout,receivePubRelTimeout=$receivePubRelTimeout,receiveSubAckTimeout=$receiveSubAckTimeout)"
}
