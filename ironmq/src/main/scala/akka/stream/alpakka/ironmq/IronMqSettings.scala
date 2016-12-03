/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config

object IronMqSettings {

  def apply(config: Config): IronMqSettings =
    new ConfigIronMqSettings(config.getConfig("akka.stream.alpakka.ironmq"))

  def apply()(implicit as: ActorSystem): IronMqSettings =
    apply(as.settings.config)

}

trait IronMqSettings {
  def endpoint: Uri
  def projectId: String
  def token: String
}

class ConfigIronMqSettings(config: Config) extends IronMqSettings {
  override val endpoint: Uri = config.getString("endpoint")
  override val projectId: String = config.getString("credentials.project-id")
  override val token: String = config.getString("credentials.token")
}
