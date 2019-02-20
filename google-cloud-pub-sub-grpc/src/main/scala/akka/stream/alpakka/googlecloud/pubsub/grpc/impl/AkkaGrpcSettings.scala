/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
    val sslConfig = config.rootCa.fold("") { rootCa =>
      s"""
      |ssl-config {
      |  disabledKeyAlgorithms = []
      |  trustManager = {
      |    stores = [
      |      { type = "PEM", path = "$rootCa", classpath = true }
      |    ]
      |  }
      |}""".stripMargin
    }

    val akkaGrpcConfig = s"""
      |host = "${config.host}"
      |port = ${config.port}
      |
      |$sslConfig
      |""".stripMargin

    val settings =
      GrpcClientSettings.fromConfig(
        ConfigFactory
          .parseString(akkaGrpcConfig)
          .withFallback(sys.settings.config.getConfig("akka.grpc.client.\"*\""))
      )

    val setTls = (settings: GrpcClientSettings) =>
      config.rootCa
        .fold(settings.withTls(false))(_ => settings.withTls(true))

    val setCallCredentials = (settings: GrpcClientSettings) =>
      config.callCredentials.fold(settings)(settings.withCallCredentials)

    Seq(setTls, setCallCredentials).foldLeft(settings) {
      case (s, f) => f(s)
    }
  }
}
