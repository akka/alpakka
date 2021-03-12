/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.http.javadsl.{model => jm}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{Http, HttpsConnectionContext}
import akka.http.{javadsl => jh}
import akka.stream.alpakka.google.auth.Credentials
import akka.stream.alpakka.google.http.{ForwardProxyHttpsContext, ForwardProxyPoolSettings}
import akka.stream.alpakka.google.syntax._
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import java.time
import java.util.Optional
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

object GoogleSettings {
  val ConfigPath = "alpakka.google"

  /**
   * Reads from the given config.
   */
  def apply(c: Config)(implicit system: ClassicActorSystemProvider): GoogleSettings = {

    val credentials = Credentials(c.getConfig("credentials"))
    val requestSettings = RequestSettings(c)
    val retrySettings = RetrySettings(c.getConfig("retry-settings"))
    val maybeForwardProxy =
      if (c.hasPath("forward-proxy"))
        Some(ForwardProxy(c.getConfig("forward-proxy")))
      else
        None

    GoogleSettings(credentials.projectId, credentials, requestSettings, retrySettings, maybeForwardProxy)
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config, system: ClassicActorSystemProvider) =
    apply(c)(system)

  /**
   * Scala API: Creates [[GoogleSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def apply()(implicit system: ClassicActorSystemProvider, dummy: DummyImplicit): GoogleSettings = apply(system)

  /**
   * Scala API: Creates [[GoogleSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ClassicActorSystemProvider): GoogleSettings = GoogleExt(system.classicSystem).settings

  implicit def settings(implicit system: ClassicActorSystemProvider): GoogleSettings = apply(system)

  /**
   * Java API: Creates [[GoogleSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def create(system: ClassicActorSystemProvider): GoogleSettings = apply(system)

  /**
   * Java API
   */
  def create(projectId: String,
             credentials: Credentials,
             requestSettings: RequestSettings,
             retrySettings: RetrySettings,
             forwardProxy: Optional[ForwardProxy]) =
    GoogleSettings(projectId, credentials, requestSettings, retrySettings, forwardProxy.asScala)

}

final case class GoogleSettings @InternalApi private (projectId: String,
                                                      credentials: Credentials,
                                                      requestSettings: RequestSettings,
                                                      retrySettings: RetrySettings,
                                                      forwardProxy: Option[ForwardProxy]) {
  def getProjectId = projectId
  def getCredentials = credentials
  def getRequestSettings = requestSettings
  def getRetrySettings = retrySettings
  def getForwardProxy = forwardProxy

  def withProjectId(projectId: String) =
    copy(projectId = projectId)
  def withCredentials(credentials: Credentials) =
    copy(credentials = credentials)
  def withRequestSettings(requestSettings: RequestSettings) =
    copy(requestSettings = requestSettings)
  def withRetrySettings(retrySettings: RetrySettings) =
    copy(retrySettings = retrySettings)
  def withForwardProxy(forwardProxy: Optional[ForwardProxy]) =
    copy(forwardProxy = forwardProxy.asScala)
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
             credentials: Optional[jm.headers.BasicHttpCredentials],
             trustPem: Optional[String],
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

object RequestSettings {

  def apply(config: Config): RequestSettings = {
    RequestSettings(
      Some(config.getString("user-ip")).filterNot(_.isEmpty),
      Some(config.getString("quota-user")).filterNot(_.isEmpty),
      config.getBoolean("pretty-print"),
      config.getBytes("upload-chunk-size")
    )
  }

  def create(config: Config) = apply(config)

  def create(userIp: Optional[String], quotaUser: Optional[String], prettyPrint: Boolean, chunkSize: Long) =
    apply(userIp.asScala, quotaUser.asScala, prettyPrint, chunkSize)
}

final case class RequestSettings @InternalApi private (
    userIp: Option[String],
    quotaUser: Option[String],
    prettyPrint: Boolean,
    uploadChunkSize: Long
) {
  def getUserIp = userIp.asJava
  def getQuotaUser = quotaUser.asJava
  def getPrettyPrint = prettyPrint
  def getUploadChunkSize = uploadChunkSize

  def withUserIp(userIp: Optional[String]) =
    copy(userIp = userIp.asScala)
  def withQuotaUser(quotaUser: Optional[String]) =
    copy(quotaUser = quotaUser.asScala)
  def withPrettyPrint(prettyPrint: Boolean) =
    copy(prettyPrint = prettyPrint)
  def withUploadChunkSize(uploadChunkSize: Long) =
    copy(uploadChunkSize = uploadChunkSize)

  // Cache query string
  private[google] def query =
    ("prettyPrint" -> prettyPrint.toString) +: ("userIp" -> userIp) ?+: ("quotaUser" -> quotaUser) ?+: Query.Empty
  private[google] val queryString = query.toString
  private[google] val `&queryString` = "&".concat(queryString)
}

object RetrySettings {

  def apply(config: Config): RetrySettings = {
    RetrySettings(
      config.getInt("max-retries"),
      config.getDuration("min-backoff").asScala,
      config.getDuration("max-backoff").asScala,
      config.getDouble("random-factor")
    )
  }

  def create(config: Config) = apply(config)

  def create(maxRetries: Int, minBackoff: time.Duration, maxBackoff: time.Duration, randomFactor: Double) =
    apply(
      maxRetries,
      minBackoff.asScala,
      maxBackoff.asScala,
      randomFactor
    )
}

final case class RetrySettings @InternalApi private (maxRetries: Int,
                                                     minBackoff: FiniteDuration,
                                                     maxBackoff: FiniteDuration,
                                                     randomFactor: Double) {
  def getMaxRetries = maxRetries
  def getMinBackoff = minBackoff.asJava
  def getMaxBackoff = maxBackoff.asJava
  def getRandomFactor = randomFactor

  def withMaxRetries(maxRetries: Int) =
    copy(maxRetries = maxRetries)
  def withMinBackoff(minBackoff: time.Duration) =
    copy(minBackoff = minBackoff.asScala)
  def withMaxBackoff(maxBackoff: time.Duration) =
    copy(maxBackoff = maxBackoff.asScala)
  def withRandomFactor(randomFactor: Double) =
    copy(randomFactor = randomFactor)
}
