/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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
                                         val clientSendBufferSize: Int = 64,
                                         val clientTerminationWatcherBufferSize: Int = 100,
                                         val commandParallelism: Int = 50,
                                         val eventParallelism: Int = 10,
                                         val receiveConnectTimeout: FiniteDuration = 5.minutes,
                                         val receiveConnAckTimeout: FiniteDuration = 30.seconds,
                                         val producerPubAckRecTimeout: FiniteDuration = 0.seconds,
                                         val producerPubCompTimeout: FiniteDuration = 0.seconds,
                                         val consumerPubAckRecTimeout: FiniteDuration = 30.seconds,
                                         val consumerPubCompTimeout: FiniteDuration = 30.seconds,
                                         val consumerPubRelTimeout: FiniteDuration = 30.seconds,
                                         val receiveSubAckTimeout: FiniteDuration = 30.seconds,
                                         val receiveUnsubAckTimeout: FiniteDuration = 30.seconds,
                                         val serverSendBufferSize: Int = 64
) {
  require(
    commandParallelism >= 2,
    s"commandParallelism of $commandParallelism must be greater than or equal to 2 to support connection replies such as pinging"
  )
  require(maxPacketSize >= 0 && maxPacketSize <= (1 << 28),
          s"maxPacketSize of $maxPacketSize must be positive and less than ${1 << 28}"
  )

  import akka.util.JavaDurationConverters._

  /**
   * Just for clients - the number of commands that can be buffered while connected to a server. Defaults
   * to 10. Any commands received beyond this will apply backpressure.
   */
  def withClientSendBufferSize(clientSendBufferSize: Int): MqttSessionSettings =
    copy(clientSendBufferSize = clientSendBufferSize)

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
   * For producers of PUBLISH, the amount of time to wait to ack/receive a QoS 1/2 publish before retrying with
   * the DUP flag set. Defaults to 0 seconds, which means republishing only occurs on reconnect.
   */
  def withProducerPubAckRecTimeout(producerPubAckRecTimeout: FiniteDuration): MqttSessionSettings =
    copy(producerPubAckRecTimeout = producerPubAckRecTimeout)

  /**
   * JAVA API
   *
   * For producers of PUBLISH, the amount of time to wait to ack/receive a QoS 1/2 publish before retrying with
   * the DUP flag set. Defaults to 0 seconds, which means republishing only occurs on reconnect.
   */
  def withProducerPubAckRecTimeout(producerPubAckRecTimeout: Duration): MqttSessionSettings =
    copy(producerPubAckRecTimeout = producerPubAckRecTimeout.asScala)

  /**
   * For producers of PUBLISH, the amount of time to wait for a server to complete a QoS 2 publish before retrying
   * with another PUBREL. Defaults to 0 seconds, which means republishing only occurs on reconnect.
   */
  def withProducerPubCompTimeout(producerPubCompTimeout: FiniteDuration): MqttSessionSettings =
    copy(producerPubCompTimeout = producerPubCompTimeout)

  /**
   * JAVA API
   *
   * For producers of PUBLISH, the amount of time to wait for a server to complete a QoS 2 publish before retrying
   * with another PUBREL. Defaults to 0 seconds, which means republishing only occurs on reconnect.
   */
  def withProducerPubCompTimeout(producerPubCompTimeout: Duration): MqttSessionSettings =
    copy(producerPubCompTimeout = producerPubCompTimeout.asScala)

  /**
   * For consumers of PUBLISH, the amount of time to wait before receiving an ack/receive command locally in reply
   * to a QoS 1/2 publish event before failing. Defaults to 30 seconds.
   */
  def withConsumerPubAckRecTimeout(consumerPubAckRecTimeout: FiniteDuration): MqttSessionSettings =
    copy(consumerPubAckRecTimeout = consumerPubAckRecTimeout)

  /**
   * JAVA API
   *
   * For consumers of PUBLISH, the amount of time to wait before receiving an ack/receive command locally in reply
   * to a QoS 1/2 publish event before failing. Defaults to 30 seconds.
   */
  def withConsumerPubAckRecTimeout(consumerPubAckRecTimeout: Duration): MqttSessionSettings =
    copy(consumerPubAckRecTimeout = consumerPubAckRecTimeout.asScala)

  /**
   * For consumers of PUBLISH, the amount of time to wait before receiving a complete command locally in reply to a
   * QoS 2 publish event before failing. Defaults to 30 seconds.
   */
  def withConsumerPubCompTimeout(consumerPubCompTimeout: FiniteDuration): MqttSessionSettings =
    copy(consumerPubCompTimeout = consumerPubCompTimeout)

  /**
   * JAVA API
   *
   * For consumers of PUBLISH, the amount of time to wait before receiving a complete command locally in reply to a
   * QoS 2 publish event before failing. Defaults to 30 seconds.
   */
  def withConsumerPubCompTimeout(consumerPubCompTimeout: Duration): MqttSessionSettings =
    copy(consumerPubCompTimeout = consumerPubCompTimeout.asScala)

  /**
   * For consumers of PUBLISH, the amount of time to wait for a server to release a QoS 2 publish before failing.
   * Defaults to 30 seconds.
   */
  def withConsumerPubRelTimeout(consumerPubRelTimeout: FiniteDuration): MqttSessionSettings =
    copy(consumerPubRelTimeout = consumerPubRelTimeout)

  /**
   * JAVA API
   *
   * For consumers of PUBLISH, the amount of time to wait for a server to release a QoS 2 publish before failing.
   * Defaults to 30 seconds.
   */
  def withConsumerPubRelTimeout(consumerPubRelTimeout: Duration): MqttSessionSettings =
    copy(consumerPubRelTimeout = consumerPubRelTimeout.asScala)

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
   * to 100. Any commands received beyond this will apply backpressure.
   */
  def withServerSendBufferSize(serverSendBufferSize: Int): MqttSessionSettings =
    copy(serverSendBufferSize = serverSendBufferSize)

  private def copy(maxPacketSize: Int = maxPacketSize,
                   clientSendBufferSize: Int = clientSendBufferSize,
                   clientTerminationWatcherBufferSize: Int = clientTerminationWatcherBufferSize,
                   commandParallelism: Int = commandParallelism,
                   eventParallelism: Int = eventParallelism,
                   receiveConnectTimeout: FiniteDuration = receiveConnectTimeout,
                   receiveConnAckTimeout: FiniteDuration = receiveConnAckTimeout,
                   producerPubAckRecTimeout: FiniteDuration = producerPubAckRecTimeout,
                   producerPubCompTimeout: FiniteDuration = producerPubCompTimeout,
                   consumerPubAckRecTimeout: FiniteDuration = consumerPubAckRecTimeout,
                   consumerPubCompTimeout: FiniteDuration = consumerPubCompTimeout,
                   consumerPubRelTimeout: FiniteDuration = consumerPubRelTimeout,
                   receiveSubAckTimeout: FiniteDuration = receiveSubAckTimeout,
                   receiveUnsubAckTimeout: FiniteDuration = receiveUnsubAckTimeout,
                   serverSendBufferSize: Int = serverSendBufferSize
  ) =
    new MqttSessionSettings(
      maxPacketSize,
      clientSendBufferSize,
      clientTerminationWatcherBufferSize,
      commandParallelism,
      eventParallelism,
      receiveConnectTimeout,
      receiveConnAckTimeout,
      producerPubAckRecTimeout,
      producerPubCompTimeout,
      consumerPubAckRecTimeout,
      consumerPubCompTimeout,
      consumerPubRelTimeout,
      receiveSubAckTimeout,
      receiveUnsubAckTimeout,
      serverSendBufferSize
    )

  override def toString: String =
    "MqttSessionSettings(" +
    s"maxPacketSize=$maxPacketSize," +
    s"clientSendBufferSize=$clientSendBufferSize," +
    s"clientTerminationWatcherBufferSize=$clientTerminationWatcherBufferSize," +
    s"commandParallelism=$commandParallelism," +
    s"eventParallelism=$eventParallelism," +
    s"receiveConnectTimeout=${receiveConnectTimeout.toCoarsest}," +
    s"receiveConnAckTimeout=${receiveConnAckTimeout.toCoarsest}," +
    s"receivePubAckRecTimeout=${producerPubAckRecTimeout.toCoarsest}," +
    s"receivePubCompTimeout=${producerPubCompTimeout.toCoarsest}," +
    s"receivePubAckRecTimeout=${consumerPubAckRecTimeout.toCoarsest}," +
    s"receivePubCompTimeout=${consumerPubCompTimeout.toCoarsest}," +
    s"receivePubRelTimeout=${consumerPubRelTimeout.toCoarsest}," +
    s"receiveSubAckTimeout=${receiveSubAckTimeout.toCoarsest}," +
    s"receiveUnsubAckTimeout=${receiveUnsubAckTimeout.toCoarsest}," +
    s"serverSendBufferSize=$serverSendBufferSize" +
    ")"
}
