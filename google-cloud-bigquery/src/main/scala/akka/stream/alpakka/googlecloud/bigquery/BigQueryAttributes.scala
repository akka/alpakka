/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.stream.Attributes.Attribute
import akka.stream.{Attributes, Materializer}

/**
 * Akka Stream [[Attributes]] that are used when materializing BigQuery stream blueprints.
 */
object BigQueryAttributes {

  /**
   * [[BigQuerySettings]] to use for the BigQuery stream
   */
  def settings(settings: BigQuerySettings): Attributes = Attributes(BigQuerySettingsValue(settings))

  /**
   * Config path which will be used to resolve [[BigQuerySettings]]
   */
  def settingsPath(path: String): Attributes = Attributes(BigQuerySettingsPath(path))

  /**
   * Resolves the most specific [[BigQuerySettings]] for some [[Attributes]]
   */
  def resolveSettings(mat: Materializer, attr: Attributes): BigQuerySettings =
    attr.attributeList.collectFirst {
      case BigQuerySettingsValue(settings) => settings
      case BigQuerySettingsPath(path) => BigQueryExt(mat.system).settings(path)
    } getOrElse {
      BigQueryExt(mat.system).settings
    }

  private final case class BigQuerySettingsValue(settings: BigQuerySettings) extends Attribute
  private final case class BigQuerySettingsPath(path: String) extends Attribute
}
