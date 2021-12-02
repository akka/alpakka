/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing GCStorage stream blueprints.
 * @deprecated Use [[akka.stream.alpakka.google.GoogleAttributes]]
 */
@deprecated("Use akka.stream.alpakka.google.GoogleAttributes", "3.0.0")
@Deprecated
object GCStorageAttributes {

  /**
   * Settings to use for the GCStorage stream
   */
  def settings(settings: GCStorageSettings): Attributes = Attributes(GCStorageSettingsValue(settings))

  /**
   * Config path which will be used to resolve required GCStorage settings
   */
  def settingsPath(path: String): Attributes = Attributes(GCStorageSettingsPath(path))
}

/**
 * @deprecated Use [[akka.stream.alpakka.google.GoogleAttributes]]
 */
@deprecated("Use akka.stream.alpakka.google.GoogleAttributes", "3.0.0")
@Deprecated
final class GCStorageSettingsPath private (val path: String) extends Attribute

/**
 * @deprecated Use [[akka.stream.alpakka.google.GoogleAttributes]]
 */
@deprecated("Use akka.stream.alpakka.google.GoogleAttributes", "3.0.0")
@Deprecated
object GCStorageSettingsPath {
  val Default = GCStorageSettingsPath(GCStorageSettings.ConfigPath)

  def apply(path: String) = new GCStorageSettingsPath(path)
}

/**
 * @deprecated Use [[akka.stream.alpakka.google.GoogleAttributes]]
 */
@deprecated("Use akka.stream.alpakka.google.GoogleAttributes", "3.0.0")
@Deprecated
final class GCStorageSettingsValue private (val settings: GCStorageSettings) extends Attribute

/**
 * @deprecated Use [[akka.stream.alpakka.google.GoogleAttributes]]
 */
@deprecated("Use akka.stream.alpakka.google.GoogleAttributes", "3.0.0")
@Deprecated
object GCStorageSettingsValue {
  def apply(settings: GCStorageSettings) = new GCStorageSettingsValue(settings)
}
