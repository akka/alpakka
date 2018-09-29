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
  def apply(maxPacketSize: Int, receiveConnAckTimeout: FiniteDuration): MqttSessionSettings =
    new MqttSessionSettings(maxPacketSize, receiveConnAckTimeout)

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(maxPacketSize: Int, receiveConnAckTimeout: Duration): MqttSessionSettings =
    MqttSessionSettings(maxPacketSize, FiniteDuration(receiveConnAckTimeout.getNano, TimeUnit.NANOSECONDS))
}

final class MqttSessionSettings private (val maxPacketSize: Int, val receiveConnAckTimeout: FiniteDuration) {

  require(maxPacketSize >= 0 && maxPacketSize <= 0xffff,
          s"maxPacketSize of $maxPacketSize must be positive and less than ${0xffff}")

  def withMaxPacketSize(maxPacketSize: Int): MqttSessionSettings = copy(maxPacketSize = maxPacketSize)

  def withReceiveConnAckTimeout(receiveConnAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout)

  private def copy(maxPacketSize: Int = maxPacketSize, receiveConnAckTimeout: FiniteDuration = receiveConnAckTimeout) =
    new MqttSessionSettings(maxPacketSize, receiveConnAckTimeout)

  override def toString: String =
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,receiveConnAckTimeout=$receiveConnAckTimeout)"
}
