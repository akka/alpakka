/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing GCStorage stream blueprints.
 */
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

final class GCStorageSettingsPath private (val path: String) extends Attribute
object GCStorageSettingsPath {
  val Default = GCStorageSettingsPath(GCStorageSettings.ConfigPath)

  def apply(path: String) = new GCStorageSettingsPath(path)
}

final class GCStorageSettingsValue private (val settings: GCStorageSettings) extends Attribute
object GCStorageSettingsValue {
  def apply(settings: GCStorageSettings) = new GCStorageSettingsValue(settings)
}
