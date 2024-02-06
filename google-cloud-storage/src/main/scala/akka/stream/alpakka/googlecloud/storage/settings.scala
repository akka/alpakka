/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import com.typesafe.config.Config

import java.util.Objects

object GCSSettings {
  val ConfigPath = "alpakka.google.cloud-storage"

  /**
   * Reads from the given config.
   */
  def apply(c: Config): GCSSettings = {
    val endpointUrl = c.getString("endpoint-url")
    val basePath = c.getString("base-path")
    new GCSSettings(endpointUrl, basePath)
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config): GCSSettings = apply(c)

  /** Scala API */
  def apply(endpointUrl: String, basePath: String): GCSSettings =
    new GCSSettings(endpointUrl, basePath)

  /** Java API */
  def create(endpointUrl: String, basePath: String): GCSSettings =
    apply(endpointUrl, basePath)

  /**
   * Scala API: Creates [[GCSSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def apply()(implicit system: ClassicActorSystemProvider): GCSSettings = apply(system.classicSystem)

  /**
   * Scala API: Creates [[GCSSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ActorSystem): GCSSettings = apply(system.settings.config.getConfig(ConfigPath))

  /**
   * Java API: Creates [[GCSSettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def create(system: ClassicActorSystemProvider): GCSSettings = apply(system.classicSystem)

  /**
   * Java API: Creates [[GCSSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): GCSSettings = apply(system)
}

final class GCSSettings private (val endpointUrl: String, val basePath: String) {

  private def copy(endpointUrl: String = endpointUrl, basePath: String = basePath): GCSSettings =
    new GCSSettings(endpointUrl, basePath)

  def withEndpointUrl(value: String): GCSSettings = copy(endpointUrl = value)

  def withBasePath(value: String): GCSSettings = copy(basePath = value)

  /** Java API */
  def getEndpointUrl: String = endpointUrl

  /** Java API */
  def getBasePath: String = basePath

  override def toString: String =
    "GCSSettings(" +
    s"endpointUrl=$endpointUrl," +
    s"basePath=$basePath)"

  override def equals(other: Any): Boolean = other match {
    case that: GCSSettings =>
      Objects.equals(this.endpointUrl, that.endpointUrl) &&
        Objects.equals(this.basePath, that.basePath)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(this.endpointUrl, this.basePath)
}
