/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import java.util.Objects

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession

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
  def apply(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    new ForwardProxy(host, port, credentials)

  /** Java API */
  def create(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    apply(host, port, credentials)

}

final class ForwardProxy private (val host: String, val port: Int, val credentials: Option[ForwardProxyCredentials]) {

  /** Java API */
  def getHost: String = host

  /** Java API */
  def getPort: Int = port

  /** Java API */
  def getCredentials: java.util.Optional[ForwardProxyCredentials] = credentials.asJava

  def withHost(host: String) = copy(host = host)
  def withPort(port: Int) = copy(port = port)
  def withCredentials(credentials: ForwardProxyCredentials) = copy(credentials = Option(credentials))

  private def copy(host: String = host, port: Int = port, credentials: Option[ForwardProxyCredentials] = credentials) =
    new ForwardProxy(host, port, credentials)

  override def toString =
    "ForwardProxy(" +
      s"host=$host," +
      s"port=$port," +
      s"credentials=$credentials" +
      ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxy =>
      Objects.equals(this.host, that.host) &&
        Objects.equals(this.port, that.port) &&
        Objects.equals(this.credentials, that.credentials)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(host, Int.box(port), credentials)
}

object BigQueryConfig {

  /**
   * Java API
   */
  def create(clientEmail: String,
             privateKey: String,
             projectId: String,
             dataset: String,
             actorSystem: ActorSystem): BigQueryConfig = {
    apply(clientEmail, privateKey, projectId, dataset)(actorSystem)
  }

  def apply(clientEmail: String, privateKey: String, projectId: String, dataset: String)(
      implicit actorSystem: ActorSystem
  ): BigQueryConfig = {
    val session = GoogleSession(clientEmail, privateKey, actorSystem)
    new BigQueryConfig(projectId, dataset, session)
  }

}

class BigQueryConfig(val projectId: String,
                     val dataset: String,
                     @InternalApi private[bigquery] val session: GoogleSession)
