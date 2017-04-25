/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.ironmq.ConfigIronMqSettings.ConfigConsumerSettings
import akka.stream.alpakka.ironmq.IronMqSettings.ConsumerSettings
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

/**
 * IronMQ settings. To a detailed documentation please refer to the reference.conf.
 */
abstract class IronMqSettings {

  /**
   * The IronMq endpoint. It is available on the IronMQ project page and change based on availability zone and region.
   */
  def endpoint: Uri

  /**
   * The IronMq project id, it is available on the IronMQ hud.
   */
  def projectId: String

  /**
   * The IronMq authentication token, it is available on the IronMQ hud.
   */
  def token: String

  /**
   * The IronMq consumer settings.
   */
  def consumerSettings: ConsumerSettings
}

object IronMqSettings {

  trait ConsumerSettings {

    /**
     * The buffer size limit where a new batch of message will be consumed from the queue.
     */
    def bufferMinSize: Int

    /**
     * The maximum number of buffered messages.
     */
    def bufferMaxSize: Int

    /**
     * The interval of time between each poll loop.
     */
    def fetchInterval: FiniteDuration

    /**
     * The amount of time the consumer will wait for the messages to be available on the queue. The IronMQ time unit is
     * the second so any other value is approximated to the second.
     */
    def pollTimeout: FiniteDuration

    /**
     * The amount of time the consumer will reserve the message from. It should be higher that the time needed to
     * process the message otherwise the same message will be processed multiple times. Again the IronMq time unit is
     * the second.
     */
    def reservationTimeout: FiniteDuration
  }

  /**
   * Will create a [[IronMqSettings]] from a config.
   */
  def apply(config: Config): IronMqSettings =
    new ConfigIronMqSettings(config)

  /**
   * Will create a [[IronMqSettings]] from a ActorSystem using the default config path `akka.stream.alpakka.ironmq`.
   */
  def apply()(implicit as: ActorSystem): IronMqSettings =
    apply(as.settings.config.getConfig("akka.stream.alpakka.ironmq"))

  /**
   * Java API.
   */
  def create(config: Config): IronMqSettings = apply(config)

  /**
   * Java API.
   */
  def create(as: ActorSystem): IronMqSettings = apply()(as)
}

object ConfigIronMqSettings {

  class ConfigConsumerSettings(config: Config) extends ConsumerSettings {
    override val bufferMinSize: Int = config.getInt("buffer-min-size")
    override val bufferMaxSize: Int = config.getInt("buffer-max-size")
    override val fetchInterval: FiniteDuration = config.getDuration("fetch-interval").asScala
    override val pollTimeout: FiniteDuration = config.getDuration("poll-timeout").asScala
    override val reservationTimeout: FiniteDuration = config.getDuration("reservation-timeout").asScala
  }

}

class ConfigIronMqSettings(config: Config) extends IronMqSettings {
  override val endpoint: Uri = config.getString("endpoint")
  override val projectId: String = config.getString("credentials.project-id")
  override val token: String = config.getString("credentials.token")

  override def consumerSettings: ConsumerSettings = new ConfigConsumerSettings(config.getConfig("consumer"))
}
