/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
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
trait IronMqSettings {
  def endpoint: Uri

  def projectId: String

  def token: String

  def consumerSettings: ConsumerSettings
}

object IronMqSettings {

  trait ConsumerSettings {
    def bufferMinSize: Int

    def bufferMaxSize: Int

    def fetchInterval: FiniteDuration

    def pollTimeout: FiniteDuration

    def reservationTimeout: FiniteDuration
  }

  def apply(config: Config): IronMqSettings =
    new ConfigIronMqSettings(config.getConfig("akka.stream.alpakka.ironmq"))

  def apply()(implicit as: ActorSystem): IronMqSettings =
    apply(as.settings.config)

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
