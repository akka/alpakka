/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}

/**
 * Manages one [[StorageSettings]] per `ActorSystem`.
 */
final class StorageExt private (sys: ExtendedActorSystem) extends Extension {
  val settings: StorageSettings = settings(StorageSettings.ConfigPath)

  def settings(prefix: String): StorageSettings = StorageSettings(sys.settings.config.getConfig(prefix))
}

object StorageExt extends ExtensionId[StorageExt] with ExtensionIdProvider {
  override def lookup: StorageExt.type = StorageExt
  override def createExtension(system: ExtendedActorSystem) = new StorageExt(system)

  /**
   * Java API.
   * Get the Storage extension with the classic actors API.
   */
  override def get(system: ActorSystem): StorageExt = super.apply(system)

  /**
   * Java API.
   * Get the Storage extension with the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): StorageExt = super.apply(system)
}
