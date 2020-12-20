/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{Http, HttpsConnectionContext}
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryExt
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.{
  ComputeEngineCredentials,
  CredentialsProvider,
  ServiceAccountCredentials
}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.{ForwardProxyHttpsContext, ForwardProxyPoolSettings}
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration._

object BigQuerySettings {
  val ConfigPath = "alpakka.google.bigquery"

  /**
   * Reads from the given config.
   */
  def apply(c: Config)(implicit system: ClassicActorSystemProvider): BigQuerySettings = {

    val credentialsProvider = c.getString("credentials.provider") match {
      case "service-account" => ServiceAccountCredentials(c.getConfig("credentials.service-account"))
      case "compute-engine" =>
        val timeout = c.getDuration("credentials.compute-engine.timeout").asScala
        Await.result(ComputeEngineCredentials(), timeout)
    }

    val maybeForwardProxy =
      if (c.hasPath("forward-proxy"))
        Some(ForwardProxy(c.getConfig("forward-proxy")))
      else
        None

    val loadJobSettings = LoadJobSettings(c.getConfig("load-job"))
    val retrySettings = RetrySettings(c.getConfig("retry-settings"))

    BigQuerySettings(credentialsProvider.projectId,
                     credentialsProvider,
                     maybeForwardProxy,
                     loadJobSettings,
                     retrySettings)
  }

  /**
   * Scala API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def apply()(implicit system: ClassicActorSystemProvider, dummy: DummyImplicit): BigQuerySettings = apply(system)

  /**
   * Scala API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ClassicActorSystemProvider): BigQuerySettings = BigQueryExt(system.classicSystem).settings

  implicit def settings(implicit system: ClassicActorSystemProvider): BigQuerySettings = apply(system)

  /**
   * Java API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def create(system: ClassicActorSystemProvider): BigQuerySettings = apply(system)

}

final case class BigQuerySettings(projectId: String,
                                  credentialsProvider: CredentialsProvider,
                                  forwardProxy: Option[ForwardProxy],
                                  loadJobSettings: LoadJobSettings,
                                  retrySettings: RetrySettings)

object ForwardProxy {

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): ForwardProxy = {
    val scheme =
      if (c.hasPath("scheme")) c.getString("scheme")
      else "https"

    val maybeCredentials =
      if (c.hasPath("credentials"))
        Some(BasicHttpCredentials(c.getString("credentials.username"), c.getString("credentials.password")))
      else None

    val maybeTrustPem =
      if (c.hasPath("trust-pem"))
        Some(c.getString("trust-pem"))
      else
        None

    ForwardProxy(scheme, c.getString("host"), c.getInt("port"), maybeCredentials, maybeTrustPem)
  }

  def apply(scheme: String,
            host: String,
            port: Int,
            credentials: Option[BasicHttpCredentials],
            trustPem: Option[String])(implicit system: ClassicActorSystemProvider): ForwardProxy = {
    ForwardProxy(
      trustPem.fold(Http(system).defaultClientHttpsContext)(ForwardProxyHttpsContext(_)),
      ForwardProxyPoolSettings(scheme, host, port, credentials)(system.classicSystem)
    )
  }

}

final case class ForwardProxy(connectionContext: HttpsConnectionContext, poolSettings: ConnectionPoolSettings)

object LoadJobSettings {

  def apply(config: Config): LoadJobSettings = {
    LoadJobSettings(
      config.getBytes("chunk-size"),
      config.getDuration("per-table-quota").asScala
    )
  }
}

final case class LoadJobSettings(chunkSize: Long, perTableQuota: FiniteDuration)

object RetrySettings {

  def apply(config: Config): RetrySettings = {
    RetrySettings(
      config.getInt("max-retries"),
      FiniteDuration(config.getDuration("min-backoff").toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(config.getDuration("max-backoff").toNanos, TimeUnit.NANOSECONDS),
      config.getDouble("random-factor")
    )
  }
}

final case class RetrySettings(maxRetries: Int,
                               minBackoff: FiniteDuration,
                               maxBackoff: FiniteDuration,
                               randomFactor: Double)
