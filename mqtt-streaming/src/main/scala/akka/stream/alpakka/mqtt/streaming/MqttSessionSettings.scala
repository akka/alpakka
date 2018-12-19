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

/**
 * Configuration settings for client and server usage.
 */
final class MqttSessionSettings private (val maxPacketSize: Int = 4096,
                                         val clientTerminationWatcherBufferSize: Int = 100,
                                         val commandParallelism: Int = 50,
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
  require(maxPacketSize >= 0 && maxPacketSize <= (1 << 28),
          s"maxPacketSize of $maxPacketSize must be positive and less than ${1 << 28}")

  import akka.util.JavaDurationConverters._

  /**
   * The maximum size of a packet that is allowed to be decoded. Defaults to 4k.
   */
  def withMaxPacketSize(maxPacketSize: Int): MqttSessionSettings = copy(maxPacketSize = maxPacketSize)

  /**
   * The number of commands that can be processed at a time, with a default of 50. For client usage, note that each
   * CONNECT will reduced the availability of remaining command channels for other commands by 1. For server usage,
   * each CONNACK received will reduce the availability of remaining command channels for other commands by 1.
   */
  def withCommandParallelism(commandParallelism: Int): MqttSessionSettings =
    copy(commandParallelism = commandParallelism)

  /**
   * This is the number of events that can be received in parallel at any one time, with a default of 10.
   */
  def withEventParallelism(eventParallelism: Int): MqttSessionSettings =
    copy(eventParallelism = eventParallelism)

  /**
   * For servers, the amount of time a session can be disconnected before being re-connected. Defaults to
   * 5 minutes.
   */
  def withReceiveConnectTimeout(receiveConnectTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnectTimeout = receiveConnectTimeout)

  /**
   * JAVA API
   *
   * For servers, the amount of time a session can be disconnected before being re-connected. Defaults to
   * 5 minutes.
   */
  def withReceiveConnectTimeout(receiveConnectTimeout: Duration): MqttSessionSettings =
    copy(receiveConnectTimeout = receiveConnectTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to ack a connection. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a connect event. Defaults to 30 seconds.
   */
  def withReceiveConnAckTimeout(receiveConnAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to ack a connection. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a connect event. Defaults to 30 seconds.
   */
  def withReceiveConnAckTimeout(receiveConnAckTimeout: Duration): MqttSessionSettings =
    copy(receiveConnAckTimeout = receiveConnAckTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to ack/receive a QoS 1/2 publish. For servers, the amount of time
   * to wait before receiving an ack/receive command locally in reply to a QoS 1/2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubAckRecTimeout(receivePubAckRecTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubAckRecTimeout = receivePubAckRecTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to ack/receive a QoS 1/2 publish. For servers, the amount of time
   * to wait before receiving an ack/receive command locally in reply to a QoS 1/2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubAckRecTimeout(receivePubAckRecTimeout: Duration): MqttSessionSettings =
    copy(receivePubAckRecTimeout = receivePubAckRecTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to complete a QoS 2 publish. For servers, the amount of time
   * to wait before receiving a complete command locally in reply to a QoS 2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubCompTimeout(receivePubCompTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubCompTimeout = receivePubCompTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to complete a QoS 2 publish. For servers, the amount of time
   * to wait before receiving a complete command locally in reply to a QoS 2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubCompTimeout(receivePubCompTimeout: Duration): MqttSessionSettings =
    copy(receivePubCompTimeout = receivePubCompTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to release a QoS 2 publish. For servers, the amount of time
   * to wait before receiving a release command locally in reply to a QoS 2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubRelTimeout(receivePubRelTimeout: FiniteDuration): MqttSessionSettings =
    copy(receivePubRelTimeout = receivePubRelTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to release a QoS 2 publish. For servers, the amount of time
   * to wait before receiving a release command locally in reply to a QoS 2 publish event. Defaults to 30 seconds.
   */
  def withReceivePubRelTimeout(receivePubRelTimeout: Duration): MqttSessionSettings =
    copy(receivePubRelTimeout = receivePubRelTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to ack a subscribe. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a subscribe event. Defaults to 30 seconds.
   */
  def withReceiveSubAckTimeout(receiveSubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveSubAckTimeout = receiveSubAckTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to ack a subscribe. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a subscribe event. Defaults to 30 seconds.
   */
  def withReceiveSubAckTimeout(receiveSubAckTimeout: Duration): MqttSessionSettings =
    copy(receiveSubAckTimeout = receiveSubAckTimeout.asScala)

  /**
   * For clients, the amount of time to wait for a server to ack a unsubscribe. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a unsubscribe event. Defaults to 30 seconds.
   */
  def withReceiveUnsubAckTimeout(receiveUnsubAckTimeout: FiniteDuration): MqttSessionSettings =
    copy(receiveUnsubAckTimeout = receiveUnsubAckTimeout)

  /**
   * JAVA API
   *
   * For clients, the amount of time to wait for a server to ack a unsubscribe. For servers, the amount of time
   * to wait before receiving an ack command locally in reply to a unsubscribe event. Defaults to 30 seconds.
   */
  def withReceiveUnsubAckTimeout(receiveUnsubAckTimeout: Duration): MqttSessionSettings =
    copy(receiveUnsubAckTimeout = receiveUnsubAckTimeout.asScala)

  /**
   * The maximum number of client termination event observers permitted. Defaults to 100 which should be
   * more than adequate for most situations.
   */
  def withClientTerminationWatcherBufferSize(clientTerminationWatcherBufferSize: Int): MqttSessionSettings =
    copy(clientTerminationWatcherBufferSize = clientTerminationWatcherBufferSize)

  /**
   * Just for servers - the number of commands that can be buffered while connected to a client. Defaults
   * to 100. Any commands received beyond this will be dropped.
   */
  def withServerSendBufferSize(serverSendBufferSize: Int): MqttSessionSettings =
    copy(serverSendBufferSize = serverSendBufferSize)

  private def copy(maxPacketSize: Int = maxPacketSize,
                   clientTerminationWatcherBufferSize: Int = clientTerminationWatcherBufferSize,
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
      clientTerminationWatcherBufferSize,
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
    s"MqttSessionSettings(maxPacketSize=$maxPacketSize,clientTerminationWatcherBufferSize=$clientTerminationWatcherBufferSize,commandParallelism=$commandParallelism,eventParallelism=$eventParallelism,receiveConnectTimeout=$receiveConnectTimeout,receiveConnAckTimeout=$receiveConnAckTimeout,receivePubAckRecTimeout=$receivePubAckRecTimeout,receivePubCompTimeout=$receivePubCompTimeout,receivePubRelTimeout=$receivePubRelTimeout,receiveSubAckTimeout=$receiveSubAckTimeout,receiveUnsubAckTimeout=$receiveUnsubAckTimeout,serverSendBufferSize=$serverSendBufferSize)"
}
