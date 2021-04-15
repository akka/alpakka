/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import com.typesafe.config.ConfigFactory
import io.grpc.auth.MoreCallCredentials

/**
 * Internal API
 */
@InternalApi private[bigquery] object AkkaGrpcSettings {

  def fromBigQuerySettings(
      config: BigQueryStorageSettings
  )(implicit system: ClassicActorSystemProvider): GrpcClientSettings = {
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
          .withFallback(system.classicSystem.settings.config.getConfig("akka.grpc.client.\"*\""))
      )

    val setTls = (settings: GrpcClientSettings) =>
      config.rootCa
        .fold(settings.withTls(false))(_ => settings.withTls(true))

    val googleSettings = config.googleSettings
    val executor = system.classicSystem.dispatcher

    val setCallCredentials = (settings: GrpcClientSettings) => {
      googleSettings
        .map(gs => gs.credentials.asGoogle(executor, gs.requestSettings))
        .fold(settings)(a => settings.withCallCredentials(MoreCallCredentials.from(a)))
    }

    Seq(setTls, setCallCredentials).foldLeft(settings) {
      case (s, f) => f(s)
    }
  }
}
