/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import com.typesafe.config.ConfigFactory

/**
 * Internal API
 */
@InternalApi private[grpc] object AkkaGrpcSettings {
  def fromPubSubSettings(config: PubSubSettings)(implicit sys: ActorSystem): GrpcClientSettings = {
    val akkaGrpcConfig = s"""
      |host = "${config.host}"
      |port = ${config.port}
      |use-tls = ${config.useTls}
      |trusted = "${config.rootCa.getOrElse("")}"
      |""".stripMargin

    val settings = GrpcClientSettings.fromConfig(
      ConfigFactory
        .parseString(akkaGrpcConfig)
        .withFallback(sys.settings.config.getConfig("akka.grpc.client.\"*\""))
    )

    config.callCredentials match {
      case None => settings
      case Some(creds) => settings.withCallCredentials(creds)
    }
  }
}
