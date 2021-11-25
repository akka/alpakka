/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.stream.alpakka.google.RequestSettings
import akka.stream.alpakka.google.auth.Credentials
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import com.typesafe.config.{Config, ConfigFactory}
import io.grpc.auth.MoreCallCredentials

import java.util.concurrent.Executor

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

    val setCallCredentials = (settings: GrpcClientSettings) => {
      implicit val config = system.classicSystem.settings.config
      val executor: Executor = system.classicSystem.dispatcher
      settings.withCallCredentials(MoreCallCredentials.from(credentials().asGoogle(executor, requestSettings())))
    }

    Seq(setTls, setCallCredentials).foldLeft(settings) {
      case (s, f) => f(s)
    }
  }

  def credentials()(implicit system: ClassicActorSystemProvider, config: Config): Credentials = {
    val credentialsConfig = config.getConfig("alpakka.google.credentials")
    Credentials(credentialsConfig)
  }

  def requestSettings()(implicit system: ClassicActorSystemProvider, config: Config): RequestSettings = {
    val alpakkaConfig = config.getConfig("alpakka.google")
    RequestSettings(alpakkaConfig)
  }

}
