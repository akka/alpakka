/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.stream.Attributes.Attribute
import akka.stream.{Attributes, Materializer}

/**
 * Akka Stream [[Attributes]] that are used when materializing stream blueprints containing Google connectors.
 */
object GoogleAttributes {

  /**
   * [[GoogleSettings]] to use for the stream
   */
  def settings(settings: GoogleSettings): Attributes = Attributes(GoogleSettingsValue(settings))

  /**
   * Config path which will be used to resolve [[GoogleSettings]]
   */
  def settingsPath(path: String): Attributes = Attributes(GoogleSettingsPath(path))

  /**
   * Resolves the most specific [[GoogleSettings]] for some [[Attributes]]
   */
  def resolveSettings(mat: Materializer, attr: Attributes): GoogleSettings =
    attr.get[GoogleAttribute].fold(GoogleExt(mat.system).settings) {
      case GoogleSettingsValue(settings) => settings
      case GoogleSettingsPath(path) => GoogleExt(mat.system).settings(path)
    }

  private sealed abstract class GoogleAttribute extends Attribute
  private final case class GoogleSettingsValue(settings: GoogleSettings) extends GoogleAttribute
  private final case class GoogleSettingsPath(path: String) extends GoogleAttribute
}
