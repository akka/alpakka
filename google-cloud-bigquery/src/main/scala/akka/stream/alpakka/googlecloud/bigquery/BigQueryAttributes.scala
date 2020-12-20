/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.stream.Attributes.Attribute
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryExt
import akka.stream.{Attributes, Materializer}

/**
 * Akka Stream attributes that are used when materializing BigQuery stream blueprints.
 */
object BigQueryAttributes {

  /**
   * Settings to use for the BigQuery stream
   */
  def settings(settings: BigQuerySettings): Attributes = Attributes(BigQuerySettingsValue(settings))

  /**
   * Config path which will be used to resolve required BigQuery settings
   */
  def settingsPath(path: String): Attributes = Attributes(BigQuerySettingsPath(path))

  def resolveSettings(attr: Attributes, mat: Materializer): BigQuerySettings =
    attr
      .get[BigQuerySettingsValue]
      .map(_.settings)
      .getOrElse {
        val bigQueryExtension = BigQueryExt(mat.system)
        attr
          .get[BigQuerySettingsPath]
          .map(settingsPath => bigQueryExtension.settings(settingsPath.path))
          .getOrElse(bigQueryExtension.settings)
      }
}

final case class BigQuerySettingsPath(path: String) extends Attribute
object BigQuerySettingsPath {
  val Default = BigQuerySettingsPath(BigQuerySettings.ConfigPath)
}

final case class BigQuerySettingsValue(settings: BigQuerySettings) extends Attribute
