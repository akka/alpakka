/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.http.javadsl.{model => jm}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{Http, HttpsConnectionContext}
import akka.http.{javadsl => jh}
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryExt
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.{
  ComputeEngineCredentials,
  CredentialsProvider,
  ServiceAccountCredentials
}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.{ForwardProxyHttpsContext, ForwardProxyPoolSettings}
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import java.util
import java.time
import java.util.concurrent.TimeUnit
import scala.compat.java8.OptionConverters._
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
   * Java API: Reads from the given config.
   */
  def create(c: Config, system: ClassicActorSystemProvider) =
    apply(c)(system)

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

  /**
   * Java API
   */
  def create(projectId: String,
             credentialsProvider: CredentialsProvider,
             forwardProxy: util.Optional[ForwardProxy],
             loadJobSettings: LoadJobSettings,
             retrySettings: RetrySettings) =
    BigQuerySettings(projectId, credentialsProvider, forwardProxy.asScala, loadJobSettings, retrySettings)

}

final case class BigQuerySettings @InternalApi private (projectId: String,
                                                        credentialsProvider: CredentialsProvider,
                                                        forwardProxy: Option[ForwardProxy],
                                                        loadJobSettings: LoadJobSettings,
                                                        retrySettings: RetrySettings) {
  def getProjectId = projectId
  def getCredentialsProvider = credentialsProvider
  def getForwardProxy = forwardProxy
  def getLoadJobSettings = loadJobSettings
  def getRetrySettings = retrySettings

  def withProjectId(projectId: String) =
    copy(projectId = projectId)
  def withCredentialsProvider(credentialsProvider: CredentialsProvider) =
    copy(credentialsProvider = credentialsProvider)
  def withForwardProxy(forwardProxy: util.Optional[ForwardProxy]) =
    copy(forwardProxy = forwardProxy.asScala)
  def withLoadJobSettings(loadJobSettings: LoadJobSettings) =
    copy(loadJobSettings = loadJobSettings)
  def withRetrySettings(retrySettings: RetrySettings) =
    copy(retrySettings = retrySettings)
}

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

  def create(c: Config, system: ClassicActorSystemProvider) =
    apply(c)(system)

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

  def create(scheme: String,
             host: String,
             port: Int,
             credentials: util.Optional[jm.headers.BasicHttpCredentials],
             trustPem: util.Optional[String],
             system: ClassicActorSystemProvider) =
    apply(scheme, host, port, credentials.asScala.map(_.asInstanceOf[BasicHttpCredentials]), trustPem.asScala)(system)

  def create(connectionContext: jh.HttpConnectionContext, poolSettings: jh.settings.ConnectionPoolSettings) =
    apply(connectionContext.asInstanceOf[HttpsConnectionContext], poolSettings.asInstanceOf[ConnectionPoolSettings])
}

final case class ForwardProxy @InternalApi private (connectionContext: HttpsConnectionContext,
                                                    poolSettings: ConnectionPoolSettings) {
  def getConnectionContext: jh.HttpsConnectionContext = connectionContext
  def getPoolSettings: jh.settings.ConnectionPoolSettings = poolSettings
  def withConnectionContext(connectionContext: jh.HttpConnectionContext) =
    copy(connectionContext = connectionContext.asInstanceOf[HttpsConnectionContext])
  def withPoolSettings(poolSettings: jh.settings.ConnectionPoolSettings) =
    copy(poolSettings = poolSettings.asInstanceOf[ConnectionPoolSettings])
}

object LoadJobSettings {

  def apply(config: Config): LoadJobSettings = {
    LoadJobSettings(
      config.getBytes("chunk-size"),
      config.getDuration("per-table-quota").asScala
    )
  }

  def create(config: Config) = apply(config)

  def create(chunkSize: Long, perTableQuota: time.Duration) =
    apply(chunkSize, FiniteDuration(perTableQuota.toNanos, TimeUnit.NANOSECONDS))
}

final case class LoadJobSettings @InternalApi private (chunkSize: Long, perTableQuota: FiniteDuration) {
  def getChunkSize = chunkSize
  def getPerTableQuota = time.Duration.ofNanos(perTableQuota.toNanos)

  def withChunkSize(chunkSize: Long) =
    copy(chunkSize = chunkSize)
  def withPerTableQuota(perTableQuota: time.Duration) =
    copy(perTableQuota = FiniteDuration(perTableQuota.toNanos, TimeUnit.NANOSECONDS))
}

object RetrySettings {

  def apply(config: Config): RetrySettings = {
    RetrySettings(
      config.getInt("max-retries"),
      FiniteDuration(config.getDuration("min-backoff").toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(config.getDuration("max-backoff").toNanos, TimeUnit.NANOSECONDS),
      config.getDouble("random-factor")
    )
  }

  def create(config: Config) = apply(config)

  def create(maxRetries: Int, minBackoff: time.Duration, maxBackoff: time.Duration, randomFactor: Double) =
    apply(
      maxRetries,
      FiniteDuration(minBackoff.toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(maxBackoff.toNanos, TimeUnit.NANOSECONDS),
      randomFactor
    )
}

final case class RetrySettings @InternalApi private (maxRetries: Int,
                                                     minBackoff: FiniteDuration,
                                                     maxBackoff: FiniteDuration,
                                                     randomFactor: Double) {
  def getMaxRetries = maxRetries
  def getMinBackoff = time.Duration.ofNanos(minBackoff.toNanos)
  def getMaxBackoff = time.Duration.ofNanos(maxBackoff.toNanos)
  def getRandomFactor = randomFactor

  def withMaxRetries(maxRetries: Int) =
    copy(maxRetries = maxRetries)
  def withMinBackoff(minBackoff: time.Duration) =
    copy(minBackoff = FiniteDuration(minBackoff.toNanos, TimeUnit.NANOSECONDS))
  def withMaxBackoff(maxBackoff: time.Duration) =
    copy(maxBackoff = FiniteDuration(maxBackoff.toNanos, TimeUnit.NANOSECONDS))
  def withRandomFactor(randomFactor: Double) =
    copy(randomFactor = randomFactor)
}
