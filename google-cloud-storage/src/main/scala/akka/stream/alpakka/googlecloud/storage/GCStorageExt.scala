/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
 * Manages one [[GCStorageSettings]] per `ActorSystem`.
 */
final class GCStorageExt private (sys: ExtendedActorSystem) extends Extension {
  val settings: GCStorageSettings = settings(GCStorageSettings.ConfigPath)

  def settings(prefix: String): GCStorageSettings = GCStorageSettings(sys.settings.config.getConfig(prefix))
}

object GCStorageExt extends ExtensionId[GCStorageExt] with ExtensionIdProvider {
  override def lookup = GCStorageExt
  override def createExtension(system: ExtendedActorSystem) = new GCStorageExt(system)

  /** Java API */
  override def get(system: ActorSystem): GCStorageExt = super.get(system)
}
