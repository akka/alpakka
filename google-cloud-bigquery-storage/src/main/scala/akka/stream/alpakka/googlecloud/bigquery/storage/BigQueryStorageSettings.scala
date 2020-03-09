/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.GrpcCredentials
import com.typesafe.config.Config
import io.grpc.CallCredentials

import scala.util.Try

final class BigQueryStorageSettings private (
    val host: String,
    val port: Int,
    val rootCa: Option[String] = None,
    val callCredentials: Option[CallCredentials] = None
) {

  /**
   * Endpoint hostname where the gRPC connection is made.
   */
  def withHost(host: String): BigQueryStorageSettings = copy(host = host)

  /**
   * Endpoint port where the gRPC connection is made.
   */
  def withPort(port: Int): BigQueryStorageSettings = copy(port = port)

  /**
   * A filename on the classpath which contains the root certificate authority
   * that is going to be used to verify certificate presented by the gRPC endpoint.
   */
  def withRootCa(rootCa: String): BigQueryStorageSettings =
    copy(rootCa = Some(rootCa))

  /**
   * Credentials that are going to be used for gRPC call authorization.
   */
  def withCallCredentials(callCredentials: CallCredentials): BigQueryStorageSettings =
    copy(callCredentials = Some(callCredentials))

  private def copy(host: String = host,
                   port: Int = port,
                   rootCa: Option[String] = rootCa,
                   callCredentials: Option[CallCredentials] = callCredentials) =
    new BigQueryStorageSettings(host, port, rootCa, callCredentials)
}

object BigQueryStorageSettings {

  /**
   * Create settings for unsecure (no tls), unauthenticated (no root ca)
   * and unauthorized (no call credentials) endpoint.
   */
  def apply(host: String, port: Int): BigQueryStorageSettings =
    new BigQueryStorageSettings(host, port)

  /**
   * Create settings from config instance.
   */
  def apply(config: Config): BigQueryStorageSettings = {
    val host = config.getString("host")
    val port = config.getInt("port")

    val bigQueryConfig = BigQueryStorageSettings(host, port)

    val setRootCa = (bigQueryConfig: BigQueryStorageSettings) =>
      config.getString("rootCa") match {
        case fileName if fileName != "none" => bigQueryConfig.withRootCa(fileName)
        case _ => bigQueryConfig
      }
    val setCallCredentials = (bigQueryConfig: BigQueryStorageSettings) =>
      config.getString("callCredentials") match {
        case "google-application-default" =>
          Try(GrpcCredentials.applicationDefault())
            .map(bigQueryConfig.withCallCredentials)
            .getOrElse(bigQueryConfig)
        case _ => bigQueryConfig
      }

    Seq(setRootCa, setCallCredentials).foldLeft(bigQueryConfig) {
      case (c, f) => f(c)
    }
  }

  /**
   * Create settings from ActorSystem's config.
   */
  def apply(system: ActorSystem): BigQueryStorageSettings =
    BigQueryStorageSettings(system.settings.config.getConfig("alpakka.google.cloud.bigquery.grpc"))

  /**
   * Java API
   *
   * Create settings for unsecure (no tls), unauthenticated (no root ca)
   * and unauthorized (no call credentials) endpoint.
   */
  def create(host: String, port: Int): BigQueryStorageSettings =
    BigQueryStorageSettings(host, port)

  /**
   * Java API
   *
   * Create settings from config instance.
   */
  def create(config: Config): BigQueryStorageSettings =
    BigQueryStorageSettings(config)

  /**
   * Java API
   *
   * Create settings from ActorSystem's config.
   */
  def create(system: ActorSystem): BigQueryStorageSettings =
    BigQueryStorageSettings(system)
}
