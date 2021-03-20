/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import com.typesafe.config.ConfigFactory

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

    val setCallCredentials = (settings: GrpcClientSettings) =>
      config.callCredentials.fold(settings)(settings.withCallCredentials)

    Seq(setTls, setCallCredentials).foldLeft(settings) {
      case (s, f) => f(s)
    }
  }
}
