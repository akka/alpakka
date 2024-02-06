/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import com.typesafe.config.Config

object HmsSettings {

  private val MaxConcurrentConnections = 50
  private val IsTest = false

  val ConfigPath = "alpakka.huawei.pushkit"

  /**
   * Reads from the given config.
   */
  def apply(config: Config): HmsSettings = {
    val maybeForwardProxy =
      if (config.hasPath("forward-proxy"))
        Option(ForwardProxy(config.getConfig("forward-proxy")))
      else
        Option.empty

    val isTest =
      if (config.hasPath("test")) config.getBoolean("test")
      else IsTest
    val maxConcurrentConnections =
      if (config.hasPath("max-concurrent-connections")) config.getInt("max-concurrent-connections")
      else MaxConcurrentConnections
    HmsSettings(
      appId = config.getString("app-id"),
      appSecret = config.getString("app-secret"),
      isTest = isTest,
      maxConcurrentConnections = maxConcurrentConnections,
      maybeForwardProxy
    )
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(config: Config): HmsSettings = apply(config)

  /**
   * Scala API: Creates [[HmsSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ClassicActorSystemProvider): HmsSettings = HmsSettingExt(system.classicSystem).settings

  implicit def settings(implicit system: ClassicActorSystemProvider): HmsSettings = apply(system)

  /**
   * Java API: Creates [[HmsSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def create(system: ClassicActorSystemProvider): HmsSettings = apply(system)

  /**
   * Scala API: Creates [[HmsSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def apply()(implicit system: ClassicActorSystemProvider, dummy: DummyImplicit): HmsSettings = apply(system)

  def apply(
      appId: String,
      appSecret: String,
      isTest: Boolean = IsTest,
      maxConcurrentConnections: Int = MaxConcurrentConnections,
      forwardProxy: Option[ForwardProxy]
  ): HmsSettings =
    new HmsSettings(appId, appSecret, isTest, maxConcurrentConnections, forwardProxy)

  /**
   * Java API.
   */
  def create(
      appId: String,
      appSecret: String,
      isTest: Boolean,
      maxConcurrentConnections: Int,
      forwardProxy: ForwardProxy
  ): HmsSettings = {
    apply(appId, appSecret, isTest, maxConcurrentConnections, Option(forwardProxy))
  }

  def apply(
      appId: String,
      appSecret: String,
      isTest: Boolean,
      maxConcurrentConnections: Int
  ): HmsSettings = apply(appId, appSecret, isTest, maxConcurrentConnections, Option.empty)

  /**
   * Java API.
   */
  def create(
      appId: String,
      appSecret: String,
      isTest: Boolean,
      maxConcurrentConnections: Int
  ) = apply(appId, appSecret, isTest, maxConcurrentConnections)

  def apply(
      appId: String,
      appSecret: String
  ): HmsSettings = apply(appId, appSecret, Option.empty)

  /**
   * Java API.
   */
  def create(
      appId: String,
      appSecret: String
  ) = {
    apply(appId, appSecret)
  }

  def apply(
      appId: String,
      appSecret: String,
      forwardProxy: Option[ForwardProxy]
  ): HmsSettings = apply(appId, appSecret, forwardProxy)

  /**
   * Java API.
   */
  def create(
      appId: String,
      appSecret: String,
      forwardProxy: ForwardProxy
  ) = apply(appId, appSecret, Option(forwardProxy))

}

final case class HmsSettings @InternalApi private (
    appId: String,
    appSecret: String,
    test: Boolean,
    maxConcurrentConnections: Int,
    forwardProxy: Option[ForwardProxy]
) {

  def getAppId = appId
  def getAppSecret = appSecret
  def isTest = test
  def getMaxConcurrentConnections = maxConcurrentConnections
  def getForwardProxy = forwardProxy

  def withAppId(value: String): HmsSettings = copy(appId = value)
  def withAppSecret(value: String): HmsSettings = copy(appSecret = value)
  def withIsTest(value: Boolean): HmsSettings = if (test == value) this else copy(test = value)
  def withMaxConcurrentConnections(value: Int): HmsSettings = copy(maxConcurrentConnections = value)
  def withForwardProxy(value: ForwardProxy): HmsSettings = copy(forwardProxy = Option(value))
}

object ForwardProxy {

  /**
   * Reads from the given config.
   */
  def apply(c: Config): ForwardProxy = {
    val maybeCredentials =
      if (c.hasPath("credentials"))
        Some(ForwardProxyCredentials(c.getString("credentials.username"), c.getString("credentials.password")))
      else None
    val maybeTrustPem =
      if (c.hasPath("trust-pem"))
        Some(ForwardProxyTrustPem(c.getString("trust-pem")))
      else
        None
    ForwardProxy(c.getString("host"), c.getInt("port"), maybeCredentials, maybeTrustPem)
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(config: Config): ForwardProxy = apply(config)

  def apply(host: String,
            port: Int,
            credentials: Option[ForwardProxyCredentials],
            trustPem: Option[ForwardProxyTrustPem]
  ) =
    new ForwardProxy(host, port, credentials, trustPem)

  /**
   * Java API.
   */
  def create(host: String,
             port: Int,
             credentials: Option[ForwardProxyCredentials],
             trustPem: Option[ForwardProxyTrustPem]
  ) =
    apply(host, port, credentials, trustPem)

  def apply(host: String, port: Int) =
    new ForwardProxy(host, port, Option.empty, Option.empty)

  /**
   * Java API.
   */
  def create(host: String, port: Int) =
    apply(host, port)

  def apply(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    new ForwardProxy(host, port, credentials, Option.empty)

  /**
   * Java API.
   */
  def create(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    apply(host, port, credentials)
}

final case class ForwardProxy @InternalApi private (host: String,
                                                    port: Int,
                                                    credentials: Option[ForwardProxyCredentials],
                                                    trustPem: Option[ForwardProxyTrustPem]
) {

  def getHost = host
  def getPort = port
  def getCredentials = credentials
  def getForwardProxyTrustPem = trustPem

  def withHost(host: String) = copy(host = host)
  def withPort(port: Int) = copy(port = port)
  def withCredentials(credentials: ForwardProxyCredentials) = copy(credentials = Option(credentials))
  def withTrustPem(trustPem: ForwardProxyTrustPem) = copy(trustPem = Option(trustPem))
}

object ForwardProxyCredentials {

  /** Scala API */
  def apply(username: String, password: String): ForwardProxyCredentials =
    new ForwardProxyCredentials(username, password)

  /** Java API */
  def create(username: String, password: String): ForwardProxyCredentials =
    apply(username, password)

}

final case class ForwardProxyCredentials @InternalApi private (username: String, password: String) {

  def getUsername: String = username
  def getPassword: String = password

  def withUsername(username: String) = copy(username = username)
  def withPassword(password: String) = copy(password = password)
}

object ForwardProxyTrustPem {

  /** Scala API */
  def apply(pemPath: String): ForwardProxyTrustPem =
    new ForwardProxyTrustPem(pemPath)

  /** Java API */
  def create(pemPath: String): ForwardProxyTrustPem =
    apply(pemPath)
}

final case class ForwardProxyTrustPem @InternalApi private (pemPath: String) {
  def getPemPath: String = pemPath
}
