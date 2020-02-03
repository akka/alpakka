/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import com.typesafe.config.Config

final class GCStorageSettings private (
    val projectId: String,
    val clientEmail: String,
    val privateKey: String,
    val baseUrl: String,
    val basePath: String,
    val tokenUrl: String,
    val tokenScope: String
) {

  /** Java API */
  def getProjectId: String = projectId

  /** Java API */
  def getClientEmail: String = clientEmail

  /** Java API */
  def getPrivateKey: String = privateKey

  /** Java API */
  def getBaseUrl: String = baseUrl

  /** Java API */
  def getBasePath: String = basePath

  /** Java API */
  def getTokenUrl: String = tokenUrl

  /** Java API */
  def getTokenScope: String = tokenScope

  def withProjectId(value: String): GCStorageSettings = copy(projectId = value)
  def withClientEmail(value: String): GCStorageSettings = copy(clientEmail = value)
  def withPrivateKey(value: String): GCStorageSettings = copy(privateKey = value)
  def withBaseUrl(value: String): GCStorageSettings = copy(baseUrl = value)
  def withBasePath(value: String): GCStorageSettings = copy(basePath = value)
  def withTokenUrl(value: String): GCStorageSettings = copy(tokenUrl = value)
  def withTokenScope(value: String): GCStorageSettings = copy(tokenScope = value)

  private def copy(
      projectId: String = projectId,
      clientEmail: String = clientEmail,
      privateKey: String = privateKey,
      baseUrl: String = baseUrl,
      basePath: String = basePath,
      tokenUrl: String = tokenUrl,
      tokenScope: String = tokenScope
  ): GCStorageSettings = new GCStorageSettings(
    projectId = projectId,
    clientEmail = clientEmail,
    privateKey = privateKey,
    baseUrl = baseUrl,
    basePath = basePath,
    tokenUrl = tokenUrl,
    tokenScope = tokenScope
  )

  override def toString =
    "GCStorageSettings(" +
    s"projectId=$projectId," +
    s"clientEmail=$clientEmail," +
    s"privateKey=$privateKey," +
    s"baseUrl=$baseUrl," +
    s"basePath=$basePath," +
    s"tokenUrl=$tokenUrl," +
    s"tokenScope=$tokenScope" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: GCStorageSettings =>
      java.util.Objects.equals(this.projectId, that.projectId) &&
      java.util.Objects.equals(this.clientEmail, that.clientEmail) &&
      java.util.Objects.equals(this.privateKey, that.privateKey) &&
      java.util.Objects.equals(this.baseUrl, that.baseUrl) &&
      java.util.Objects.equals(this.basePath, that.basePath) &&
      java.util.Objects.equals(this.tokenUrl, that.tokenUrl) &&
      java.util.Objects.equals(this.tokenScope, that.tokenScope)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(projectId, clientEmail, privateKey, baseUrl, basePath, tokenUrl, tokenScope)
}

object GCStorageSettings {
  val ConfigPath = "alpakka.google.cloud.storage"

  private val defaultBaseUrl = "https://www.googleapis.com/"
  private val defaultBasePath = "/storage/v1"
  private val defaultTokenUrl = "https://www.googleapis.com/oauth2/v4/token"
  private val defaultTokenScope = "https://www.googleapis.com/auth/devstorage.read_write"

  /**
   * Reads from the given config.
   */
  def apply(c: Config): GCStorageSettings = {
    val projectId = c.getString("project-id")
    val clientEmail = c.getString("client-email")
    val privateKey = c.getString("private-key")
    val baseUrl = c.getString("base-url")
    val basePath = c.getString("base-path")
    val tokenUrl = c.getString("token-url")
    val tokenScope = c.getString("token-scope")

    new GCStorageSettings(
      projectId,
      clientEmail,
      privateKey,
      baseUrl,
      basePath,
      tokenUrl,
      tokenScope
    )
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config): GCStorageSettings = apply(c)

  /* sample config section
  -g-c-storage-settings {
    project-id = "some text"
    client-email = "some text"
    private-key = "some text"
    base-url = "some text"
    base-path = "some text"
    token-url = "some text"
    token-scope = "some text"
  }
   */

  /** Scala API */
  def apply(
      projectId: String,
      clientEmail: String,
      privateKey: String
  ): GCStorageSettings = new GCStorageSettings(
    projectId,
    clientEmail,
    privateKey,
    defaultBaseUrl,
    defaultBasePath,
    defaultTokenUrl,
    defaultTokenScope
  )

  /** Java API */
  def create(
      projectId: String,
      clientEmail: String,
      privateKey: String
  ): GCStorageSettings = new GCStorageSettings(
    projectId,
    clientEmail,
    privateKey,
    defaultBaseUrl,
    defaultBasePath,
    defaultTokenUrl,
    defaultTokenScope
  )

  /**
   * Scala API: Creates [[GCStorageSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply()(implicit system: ActorSystem): GCStorageSettings = apply(system.settings.config.getConfig(ConfigPath))

  /**
   * Java API: Creates [[S3Settings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): GCStorageSettings = apply()(system)
}
