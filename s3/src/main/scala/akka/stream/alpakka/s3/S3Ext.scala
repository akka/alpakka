/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3
import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
 * Manages one [[S3Settings]] per `ActorSystem`.
 */
final class S3Ext private (sys: ExtendedActorSystem) extends Extension {
  def settings(prefix: String): S3Settings = S3Settings(sys.settings.config.getConfig(prefix))
  def settings(): S3Settings = settings(S3Settings.ConfigPath)
}

object S3Ext extends ExtensionId[S3Ext] with ExtensionIdProvider {
  override def lookup = S3Ext
  override def createExtension(system: ExtendedActorSystem) = new S3Ext(system)
}
