/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

import java.util.Objects

import scala.compat.java8.OptionConverters._

final class FcmSettings private (
    val clientEmail: String,
    val privateKey: String,
    val projectId: String,
    val isTest: Boolean,
    val maxConcurrentConnections: Int,
    val forwardProxy: Option[ForwardProxy] = Option.empty
) {

  def withClientEmail(value: String): FcmSettings = copy(clientEmail = value)
  def withPrivateKey(value: String): FcmSettings = copy(privateKey = value)
  def withProjectId(value: String): FcmSettings = copy(projectId = value)
  def withIsTest(value: Boolean): FcmSettings = if (isTest == value) this else copy(isTest = value)
  def withMaxConcurrentConnections(value: Int): FcmSettings = copy(maxConcurrentConnections = value)
  def withForwardProxy(value: ForwardProxy): FcmSettings = copy(forwardProxy = Option(value))

  private def copy(
      clientEmail: String = clientEmail,
      privateKey: String = privateKey,
      projectId: String = projectId,
      isTest: Boolean = isTest,
      maxConcurrentConnections: Int = maxConcurrentConnections,
      forwardProxy: Option[ForwardProxy] = forwardProxy
  ): FcmSettings =
    new FcmSettings(clientEmail = clientEmail,
                    privateKey = privateKey,
                    projectId = projectId,
                    isTest = isTest,
                    maxConcurrentConnections = maxConcurrentConnections,
                    forwardProxy = forwardProxy)

  override def toString =
    s"""FcmFlowConfig(clientEmail=$clientEmail,projectId=$projectId,isTest=$isTest,maxConcurrentConnections=$maxConcurrentConnections,forwardProxy=$forwardProxy)"""

}

object ForwardProxyTrustPem {

  /** Scala API */
  def apply(pemPath: String): ForwardProxyTrustPem =
    new ForwardProxyTrustPem(pemPath)

  /** Java API */
  def create(pemPath: String): ForwardProxyTrustPem =
    apply(pemPath)

}

final class ForwardProxyTrustPem private (val pemPath: String) {

  def getPemPath: String = pemPath

  override def toString: String =
    "ForwardProxyTrustPem(" +
    s"pemPath=$pemPath," +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxyTrustPem =>
      Objects.equals(this.pemPath, that.pemPath)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(pemPath)

}

object ForwardProxyCredentials {

  /** Scala API */
  def apply(username: String, password: String): ForwardProxyCredentials =
    new ForwardProxyCredentials(username, password)

  /** Java API */
  def create(username: String, password: String): ForwardProxyCredentials =
    apply(username, password)

}

final class ForwardProxyCredentials private (val username: String, val password: String) {

  /** Java API */
  def getUsername: String = username

  /** Java API */
  def getPassword: String = password

  def withUsername(username: String) = copy(username = username)
  def withPassword(password: String) = copy(password = password)

  private def copy(username: String = username, password: String = password) =
    new ForwardProxyCredentials(username, password)

  override def toString =
    "ForwardProxyCredentials(" +
    s"username=$username," +
    s"password=******" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxyCredentials =>
      Objects.equals(this.username, that.username) &&
      Objects.equals(this.password, that.password)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(username, password)

}

object ForwardProxy {

  /** Scala API */
  def apply(host: String, port: Int) =
    new ForwardProxy(host, port, Option.empty, Option.empty)

  def apply(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    new ForwardProxy(host, port, credentials, Option.empty)

  def apply(host: String,
            port: Int,
            credentials: Option[ForwardProxyCredentials],
            trustPem: Option[ForwardProxyTrustPem]) =
    new ForwardProxy(host, port, credentials, trustPem)

  /** Java API */
  def create(host: String, port: Int) =
    apply(host, port)

  def create(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    apply(host, port, credentials)

  def create(host: String,
             port: Int,
             credentials: Option[ForwardProxyCredentials],
             trustPem: Option[ForwardProxyTrustPem]) =
    apply(host, port, credentials, trustPem)

}

final class ForwardProxy private (val host: String,
                                  val port: Int,
                                  val credentials: Option[ForwardProxyCredentials],
                                  val trustPem: Option[ForwardProxyTrustPem]) {

  /** Java API */
  def getHost: String = host

  /** Java API */
  def getPort: Int = port

  /** Java API */
  def getCredentials: java.util.Optional[ForwardProxyCredentials] = credentials.asJava

  def getForwardProxyTrustPem: java.util.Optional[ForwardProxyTrustPem] = trustPem.asJava

  def withHost(host: String) = copy(host = host)
  def withPort(port: Int) = copy(port = port)
  def withCredentials(credentials: ForwardProxyCredentials) = copy(credentials = Option(credentials))

  private def copy(host: String = host,
                   port: Int = port,
                   credentials: Option[ForwardProxyCredentials] = credentials,
                   trustPem: Option[ForwardProxyTrustPem] = trustPem) =
    new ForwardProxy(host, port, credentials, trustPem)

  override def toString =
    "ForwardProxy(" +
    s"host=$host," +
    s"port=$port," +
    s"credentials=$credentials," +
    s"trustPem=$trustPem" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxy =>
      Objects.equals(this.host, that.host) &&
      Objects.equals(this.port, that.port) &&
      Objects.equals(this.credentials, that.credentials) &&
      Objects.equals(this.trustPem, that.trustPem)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(host, Int.box(port), credentials)
}

object FcmSettings {

  /** Scala API */
  def apply(
      clientEmail: String,
      privateKey: String,
      projectId: String
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectId,
    isTest = false,
    maxConcurrentConnections = 100
  )

  def apply(
      clientEmail: String,
      privateKey: String,
      projectId: String,
      forwardProxy: ForwardProxy
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectId,
    isTest = false,
    maxConcurrentConnections = 100,
    forwardProxy = Option(forwardProxy)
  )

  /** Java API */
  def create(clientEmail: String, privateKey: String, projectId: String): FcmSettings = {
    apply(clientEmail, privateKey, projectId)
  }

  def create(clientEmail: String, privateKey: String, projectId: String, forwardProxy: ForwardProxy): FcmSettings = {
    apply(clientEmail, privateKey, projectId, forwardProxy)
  }
}
