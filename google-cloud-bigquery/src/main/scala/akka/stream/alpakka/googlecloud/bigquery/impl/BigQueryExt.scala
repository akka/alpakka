/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings

import scala.collection.immutable.ListMap

/**
 * Manages one [[BigQuerySettings]] per `ActorSystem`.
 */
private[bigquery] final class BigQueryExt private (sys: ExtendedActorSystem) extends Extension {
  private var cachedSettings: Map[String, BigQuerySettings] = ListMap.empty
  val settings: BigQuerySettings = settings(BigQuerySettings.ConfigPath)

  def settings(prefix: String): BigQuerySettings =
    cachedSettings.getOrElse(prefix, {
      val settings = BigQuerySettings(sys.settings.config.getConfig(prefix))(sys)
      cachedSettings += prefix -> settings
      settings
    })
}

private[bigquery] object BigQueryExt extends ExtensionId[BigQueryExt] with ExtensionIdProvider {

  def apply()(implicit system: ActorSystem): BigQueryExt = super.apply(system)

  override def lookup = BigQueryExt
  override def createExtension(system: ExtendedActorSystem) = new BigQueryExt(system)

  /**
   * Java API.
   * Get the BigQuery extension with the classic actors API.
   */
  override def get(system: akka.actor.ActorSystem): BigQueryExt = super.apply(system)

  /**
   * Java API.
   * Get the BigQuery extension with the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): BigQueryExt = super.apply(system)
}
