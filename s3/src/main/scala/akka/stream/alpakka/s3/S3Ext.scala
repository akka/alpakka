/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
 * Manages one [[S3Settings]] per `ActorSystem`.
 */
final class S3Ext private (sys: ExtendedActorSystem) extends Extension {
  val settings: S3Settings = settings(S3Settings.ConfigPath)

  def settings(prefix: String): S3Settings = S3Settings(sys.settings.config.getConfig(prefix))
}

object S3Ext extends ExtensionId[S3Ext] with ExtensionIdProvider {
  override def lookup = S3Ext
  override def createExtension(system: ExtendedActorSystem) = new S3Ext(system)

  /** Java API */
  override def get(system: ActorSystem): S3Ext = super.get(system)
}
