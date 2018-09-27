/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

object SessionFlowSettings {

  /**
   * Factory method for Scala.
   */
  def apply(maxPacketSize: Int): SessionFlowSettings =
    new SessionFlowSettings(maxPacketSize)

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(maxPacketSize: Int): SessionFlowSettings =
    SessionFlowSettings(maxPacketSize)
}

final class SessionFlowSettings private (val maxPacketSize: Int) {

  require(maxPacketSize >= 0 && maxPacketSize <= 0xffff,
          s"maxPacketSize of $maxPacketSize must be positive and less than ${0xffff}")

  def withMaxPacketSize(maxPacketSize: Int): SessionFlowSettings = copy(maxPacketSize = maxPacketSize)

  private def copy(maxPacketSize: Int = maxPacketSize) = new SessionFlowSettings(maxPacketSize)

  override def toString: String =
    s"SessionFlowSettings(maxPacketSize=$maxPacketSize)"
}
