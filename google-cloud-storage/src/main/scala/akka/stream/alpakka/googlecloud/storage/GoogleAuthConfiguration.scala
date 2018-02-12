/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import com.typesafe.config.Config

object GoogleAuthConfiguration {

  private val configPath = "alpakka.google.serviceAccountFile"

  def apply(config: Config): Option[GoogleAuthConfiguration] =
    Some(config)
      .filter(_.hasPath(configPath))
      .map(_.getString(configPath))
      .filter(_.trim.nonEmpty)
      .map(GoogleAuthConfiguration(_))
}
final case class GoogleAuthConfiguration(serviceAccountFile: String)
