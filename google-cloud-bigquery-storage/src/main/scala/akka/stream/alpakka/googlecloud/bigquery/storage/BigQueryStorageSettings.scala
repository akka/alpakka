/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import akka.actor.ClassicActorSystemProvider
import akka.stream.alpakka.google.GoogleSettings
import com.typesafe.config.Config

final class BigQueryStorageSettings private (
    val host: String,
    val port: Int,
    val rootCa: Option[String] = None,
    val googleSettings: Option[GoogleSettings] = None
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
  def withGoogleSettings(googleSettings: GoogleSettings): BigQueryStorageSettings =
    copy(googleSettings = Some(googleSettings))

  private def copy(host: String = host,
                   port: Int = port,
                   rootCa: Option[String] = rootCa,
                   googleSettings: Option[GoogleSettings] = googleSettings) =
    new BigQueryStorageSettings(host, port, rootCa, googleSettings)

  override def toString: String =
    "BigQueryStorageSettings(" +
    s"host=$host, " +
    s"port=$port, " +
    s"rootCa=$rootCa, " +
    s"googleSettings=$googleSettings"
  ")"
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
  def apply(config: Config)(implicit system: ClassicActorSystemProvider): BigQueryStorageSettings = {
    val host = config.getString("host")
    val port = config.getInt("port")

    val bigQueryConfig = BigQueryStorageSettings(host, port)

    val setRootCa = (bigQueryConfig: BigQueryStorageSettings) =>
      config.getString("rootCa") match {
        case fileName if fileName != "none" => bigQueryConfig.withRootCa(fileName)
        case _ => bigQueryConfig
      }

    val setGoogleSettings = (bigQueryConfig: BigQueryStorageSettings) => {
      config.hasPath("alpakka.google") match {
        case boolean: Boolean =>
          val googleSettings = GoogleSettings.apply(config)
          bigQueryConfig.withGoogleSettings(googleSettings)
        case _ => bigQueryConfig
      }
    }

    Seq(setRootCa, setGoogleSettings).foldLeft(bigQueryConfig) {
      case (c, f) => f(c)
    }
  }

  /**
   * Create settings from ActorSystem's config.
   */
  def apply(system: ClassicActorSystemProvider): BigQueryStorageSettings =
    BigQueryStorageSettings(system.classicSystem.settings.config.getConfig("alpakka.google.cloud.bigquery.grpc"))(
      system
    )

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
  def create(config: Config, system: ClassicActorSystemProvider): BigQueryStorageSettings =
    BigQueryStorageSettings(config)(system)

  /**
   * Java API
   *
   * Create settings from ActorSystem's config.
   */
  def create(system: ClassicActorSystemProvider): BigQueryStorageSettings =
    BigQueryStorageSettings(system)
}
