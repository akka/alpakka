/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing AzureStorage stream blueprints.
 */
object StorageAttributes {

  /**
   * Settings to use for the Azure Blob Storage stream
   */
  def settings(settings: StorageSettings): Attributes = Attributes(StorageSettingsValue(settings))

  /**
   * Config path which will be used to resolve required AzureStorage settings
   */
  def settingsPath(path: String): Attributes = Attributes(StorageSettingsPath(path))

  /**
   * Default settings
   */
  def defaultSettings: Attributes = Attributes(StorageSettingsPath.Default)
}

final class StorageSettingsPath private (val path: String) extends Attribute
object StorageSettingsPath {
  val Default: StorageSettingsPath = StorageSettingsPath(StorageSettings.ConfigPath)

  def apply(path: String) = new StorageSettingsPath(path)
}

final class StorageSettingsValue private (val settings: StorageSettings) extends Attribute
object StorageSettingsValue {
  def apply(settings: StorageSettings) = new StorageSettingsValue(settings)
}
