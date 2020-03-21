/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import java.util.Objects

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession

import scala.compat.java8.OptionConverters._

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

  /** Scala API */
  def apply(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    new ForwardProxy(host, port, credentials, Option.empty)

  def apply(host: String,
            port: Int,
            credentials: Option[ForwardProxyCredentials],
            trustPem: Option[ForwardProxyTrustPem]) =
    new ForwardProxy(host, port, credentials, trustPem)

  /** Java API */
  def create(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    apply(host, port, credentials)

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

object BigQueryConfig {

  /** Scala API */
  def apply(clientEmail: String, privateKey: String, projectId: String, dataset: String)(
      implicit actorSystem: ActorSystem
  ): BigQueryConfig = {
    val session = GoogleSession(clientEmail, privateKey, actorSystem, Option.empty)
    new BigQueryConfig(projectId, dataset, Option.empty, session)
  }

  def apply(clientEmail: String, privateKey: String, projectId: String, dataset: String, forwardProxy: ForwardProxy)(
      implicit actorSystem: ActorSystem
  ): BigQueryConfig = {
    val session = GoogleSession(clientEmail, privateKey, actorSystem, Option(forwardProxy))
    new BigQueryConfig(projectId, dataset, Option(forwardProxy), session)
  }

  /** Java API */
  def create(clientEmail: String,
             privateKey: String,
             projectId: String,
             dataset: String,
             actorSystem: ActorSystem): BigQueryConfig = {
    apply(clientEmail, privateKey, projectId, dataset)(actorSystem)
  }

}

@InternalApi
private[bigquery] final class BigQueryConfig(val projectId: String,
                                             val dataset: String,
                                             val forwardProxy: Option[ForwardProxy],
                                             @InternalApi private[bigquery] val session: GoogleSession) {}
