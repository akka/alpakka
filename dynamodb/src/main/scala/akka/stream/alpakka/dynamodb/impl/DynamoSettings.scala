/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem

object DynamoSettings {
  def apply(system: ActorSystem): DynamoSettings = {
    val config = system.settings.config.getConfig("akka.stream.alpakka.dynamodb")
    DynamoSettings(
      region = config getString "region",
      host = config getString "host",
      port = config getInt "port",
      parallelism = config getInt "parallelism"
    )
  }
}

case class DynamoSettings(region: String, host: String, port: Int, parallelism: Int) extends ClientSettings {
  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")
}
