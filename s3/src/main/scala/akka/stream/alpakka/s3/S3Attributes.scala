/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3
import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing S3 stream blueprints.
 */
object S3Attributes {

  /**
   * Settings to use for the S3 stream
   */
  def settings(settings: S3Settings): Attributes = Attributes(S3SettingsValue(settings))

  /**
   * Config path which will be used to resolve required S3 settings
   */
  def settingsPath(path: String): Attributes = Attributes(S3SettingsPath(path))
}

final class S3SettingsPath private (val path: String) extends Attribute
object S3SettingsPath {
  val Default = S3SettingsPath(S3Settings.ConfigPath)

  def apply(path: String) = new S3SettingsPath(path)
}

final class S3SettingsValue private (val settings: S3Settings) extends Attribute
object S3SettingsValue {
  def apply(settings: S3Settings) = new S3SettingsValue(settings)
}
