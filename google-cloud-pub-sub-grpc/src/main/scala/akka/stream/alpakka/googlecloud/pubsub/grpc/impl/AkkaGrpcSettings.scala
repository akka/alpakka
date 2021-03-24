/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import com.github.ghik.silencer.silent
import com.typesafe.config.ConfigFactory
import io.grpc.auth.MoreCallCredentials

/**
 * Internal API
 */
@InternalApi private[grpc] object AkkaGrpcSettings {
  def fromPubSubSettings(config: PubSubSettings,
                         googleSettings: GoogleSettings)(implicit sys: ActorSystem): GrpcClientSettings = {
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

    (config.callCredentials: @silent("deprecated")) match {
      case None => settings
      case Some(DeprecatedCredentials(_)) => // Deprecated credentials were loaded from config so override them
        sys.log.warning(
          "Config path alpakka.google.cloud.pubsub.grpc.callCredentials is deprecated, use alpakka.google.credentials"
        )
        val credentials = googleSettings.credentials.asGoogle(sys.dispatcher, googleSettings.requestSettings)
        settings.withCallCredentials(MoreCallCredentials.from(credentials))
      case Some(creds) => settings.withCallCredentials(creds)
    }
  }
}
