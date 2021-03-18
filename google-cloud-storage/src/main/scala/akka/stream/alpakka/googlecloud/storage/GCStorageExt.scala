/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
 * Manages one [[GCStorageSettings]] per `ActorSystem`.
 * Deprecated, please use [[akka.stream.alpakka.google.GoogleSettings]].
 */
@deprecated("Use akka.stream.alpakka.google.GoogleSettings", "3.0.0")
final class GCStorageExt private (sys: ExtendedActorSystem) extends Extension {
  val settings: GCStorageSettings = settings(GCStorageSettings.ConfigPath)

  def settings(prefix: String): GCStorageSettings = GCStorageSettings(sys.settings.config.getConfig(prefix))
}

@deprecated("Use akka.stream.alpakka.google.GoogleSettings", "3.0.0")
object GCStorageExt extends ExtensionId[GCStorageExt] with ExtensionIdProvider {
  override def lookup = GCStorageExt
  override def createExtension(system: ExtendedActorSystem) = new GCStorageExt(system)

  /**
   * Java API.
   * Get the GCS extension with the classic actors API.
   */
  override def get(system: akka.actor.ActorSystem): GCStorageExt = super.apply(system)

  /**
   * Java API.
   * Get the GCS extension with the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): GCStorageExt = super.apply(system)
}
