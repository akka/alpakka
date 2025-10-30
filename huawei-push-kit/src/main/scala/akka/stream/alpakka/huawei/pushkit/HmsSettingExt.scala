/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.huawei.pushkit

import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.annotation.InternalApi

import scala.collection.immutable.ListMap

/**
 * INTERNAL API.
 * Manages one [[HmsSettings]] per `ActorSystem`.
 */
@InternalApi
private[pushkit] final class HmsSettingExt private (sys: ExtendedActorSystem) extends Extension {
  private var cachedSettings: Map[String, HmsSettings] = ListMap.empty
  val settings: HmsSettings = settings(HmsSettings.ConfigPath)

  def settings(path: String): HmsSettings =
    cachedSettings.getOrElse(path, {
      val settings = HmsSettings(sys.settings.config.getConfig(path))
      cachedSettings += path -> settings
      settings
    })
}

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] object HmsSettingExt extends ExtensionId[HmsSettingExt] with ExtensionIdProvider {

  def apply()(implicit system: ActorSystem): HmsSettingExt = super.apply(system)

  override def lookup = HmsSettingExt
  override def createExtension(system: ExtendedActorSystem) = new HmsSettingExt(system)

  /**
   * Java API.
   * Get the HmsSettings extension with the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): HmsSettingExt = super.apply(system)
}
