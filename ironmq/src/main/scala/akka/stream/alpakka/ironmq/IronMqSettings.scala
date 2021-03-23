/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.ironmq.IronMqSettings.ConsumerSettings
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import akka.util.JavaDurationConverters._

/**
 * IronMQ settings. To a detailed documentation please refer to the reference.conf.
 */
final class IronMqSettings private (
    val endpoint: akka.http.scaladsl.model.Uri,
    val projectId: String,
    val token: String,
    val consumerSettings: ConsumerSettings
) {

  /** The IronMq endpoint. It is available on the IronMQ project page and change based on availability zone and region. */
  def withEndpoint(value: akka.http.scaladsl.model.Uri): IronMqSettings = copy(endpoint = value)

  /** The IronMq project id, it is available on the IronMQ hud. */
  def withProjectId(value: String): IronMqSettings = copy(projectId = value)

  /** The IronMq authentication token, it is available on the IronMQ hud. */
  def withToken(value: String): IronMqSettings = copy(token = value)

  /** The IronMq consumer settings. */
  def withConsumerSettings(value: ConsumerSettings): IronMqSettings = copy(consumerSettings = value)

  private def copy(
      endpoint: akka.http.scaladsl.model.Uri = endpoint,
      projectId: String = projectId,
      token: String = token,
      consumerSettings: ConsumerSettings = consumerSettings
  ): IronMqSettings = new IronMqSettings(
    endpoint = endpoint,
    projectId = projectId,
    token = token,
    consumerSettings = consumerSettings
  )

  override def toString =
    "IronMqSettings(" +
    s"endpoint=$endpoint," +
    s"projectId=$projectId," +
    s"token=$token," +
    s"consumerSettings=$consumerSettings" +
    ")"
}

object IronMqSettings {

  val ConfigPath = "alpakka.ironmq"

  final class ConsumerSettings private (
      val bufferMinSize: Int,
      val bufferMaxSize: Int,
      val fetchInterval: scala.concurrent.duration.FiniteDuration,
      val pollTimeout: scala.concurrent.duration.FiniteDuration,
      val reservationTimeout: scala.concurrent.duration.FiniteDuration
  ) {

    /** The buffer size limit where a new batch of message will be consumed from the queue. */
    def withBufferMinSize(value: Int): ConsumerSettings = copy(bufferMinSize = value)

    /** The maximum number of buffered messages. */
    def withBufferMaxSize(value: Int): ConsumerSettings = copy(bufferMaxSize = value)

    /** Scala API: The interval of time between each poll loop. */
    def withFetchInterval(value: scala.concurrent.duration.FiniteDuration): ConsumerSettings =
      copy(fetchInterval = value)

    /** Java API: The interval of time between each poll loop. */
    def withFetchInterval(value: java.time.Duration): ConsumerSettings = copy(fetchInterval = value.asScala)

    /** Scala API:
     * The amount of time the consumer will wait for the messages to be available on the queue. The IronMQ time unit is
     * the second so any other value is approximated to the second.
     */
    def withPollTimeout(value: scala.concurrent.duration.FiniteDuration): ConsumerSettings = copy(pollTimeout = value)

    /** Java API:
     * The amount of time the consumer will wait for the messages to be available on the queue. The IronMQ time unit is
     * the second so any other value is approximated to the second.
     */
    def withPollTimeout(value: java.time.Duration): ConsumerSettings = copy(pollTimeout = value.asScala)

    /** Scala API:
     * The amount of time the consumer will reserve the message from. It should be higher that the time needed to
     * process the message otherwise the same message will be processed multiple times. Again the IronMq time unit is
     * the second.
     */
    def withReservationTimeout(value: scala.concurrent.duration.FiniteDuration): ConsumerSettings =
      copy(reservationTimeout = value)

    /** Java API:
     * The amount of time the consumer will reserve the message from. It should be higher that the time needed to
     * process the message otherwise the same message will be processed multiple times. Again the IronMq time unit is
     * the second.
     */
    def withReservationTimeout(value: java.time.Duration): ConsumerSettings = copy(reservationTimeout = value.asScala)

    private def copy(
        bufferMinSize: Int = bufferMinSize,
        bufferMaxSize: Int = bufferMaxSize,
        fetchInterval: scala.concurrent.duration.FiniteDuration = fetchInterval,
        pollTimeout: scala.concurrent.duration.FiniteDuration = pollTimeout,
        reservationTimeout: scala.concurrent.duration.FiniteDuration = reservationTimeout
    ): ConsumerSettings = new ConsumerSettings(
      bufferMinSize = bufferMinSize,
      bufferMaxSize = bufferMaxSize,
      fetchInterval = fetchInterval,
      pollTimeout = pollTimeout,
      reservationTimeout = reservationTimeout
    )

    override def toString =
      "ConsumerSettings(" +
      s"bufferMinSize=$bufferMinSize," +
      s"bufferMaxSize=$bufferMaxSize," +
      s"fetchInterval=${fetchInterval.toCoarsest}," +
      s"pollTimeout=${pollTimeout.toCoarsest}," +
      s"reservationTimeout=${reservationTimeout.toCoarsest}" +
      ")"
  }

  object ConsumerSettings {
    def apply(config: Config): ConsumerSettings = {
      val bufferMinSize: Int = config.getInt("buffer-min-size")
      val bufferMaxSize: Int = config.getInt("buffer-max-size")
      val fetchInterval: FiniteDuration = config.getDuration("fetch-interval").asScala
      val pollTimeout: FiniteDuration = config.getDuration("poll-timeout").asScala
      val reservationTimeout: FiniteDuration = config.getDuration("reservation-timeout").asScala
      new ConsumerSettings(bufferMinSize, bufferMaxSize, fetchInterval, pollTimeout, reservationTimeout)
    }
  }

  /**
   * Will create a [[IronMqSettings]] from a config.
   */
  def apply(config: Config): IronMqSettings = {
    val endpoint: Uri = config.getString("endpoint")
    val projectId: String = config.getString("credentials.project-id")
    val token: String = config.getString("credentials.token")

    val consumerSettings: ConsumerSettings = ConsumerSettings(config.getConfig("consumer"))
    new IronMqSettings(endpoint, projectId, token, consumerSettings)
  }

  /**
   * Will create a [[IronMqSettings]] from a ActorSystem using the default config path `alpakka.ironmq`.
   * @deprecated Use [[ClassicActorSystemProvider]] constructor, 3.0.0
   */
  @Deprecated
  @deprecated("Use `ClassicActorSystemProvider` constructor", "3.0.0")
  def apply()(implicit as: ActorSystem): IronMqSettings =
    apply(as.settings.config.getConfig(ConfigPath))

  /**
   * Will create a [[IronMqSettings]] from a ActorSystem using the default config path `alpakka.ironmq`.
   */
  def apply(system: ClassicActorSystemProvider): IronMqSettings =
    apply(system.classicSystem.settings.config.getConfig(ConfigPath))

  /**
   * Java API.
   */
  def create(config: Config): IronMqSettings = apply(config)

  /**
   * Java API.
   */
  def create(as: ActorSystem): IronMqSettings = apply(as)

  /**
   * Java API.
   */
  def create(as: ClassicActorSystemProvider): IronMqSettings = apply(as)
}
