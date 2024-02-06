/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

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
 * Manages one [[GoogleSettings]] per `ActorSystem`.
 */
@InternalApi
private[google] final class GoogleExt private (sys: ExtendedActorSystem) extends Extension {
  private var cachedSettings: Map[String, GoogleSettings] = ListMap.empty
  val settings: GoogleSettings = settings(GoogleSettings.ConfigPath)

  def settings(path: String): GoogleSettings =
    cachedSettings.getOrElse(path, {
                               val settings = GoogleSettings(sys.settings.config.getConfig(path))(sys)
                               cachedSettings += path -> settings
                               settings
                             }
    )
}

@InternalApi
private[google] object GoogleExt extends ExtensionId[GoogleExt] with ExtensionIdProvider {

  def apply()(implicit system: ActorSystem): GoogleExt = super.apply(system)

  override def lookup = GoogleExt
  override def createExtension(system: ExtendedActorSystem) = new GoogleExt(system)

  /**
   * Java API.
   * Get the Google extension with the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): GoogleExt = super.apply(system)
}
