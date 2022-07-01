/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

final class TypesenseSettings private (val host: String, val apiKey: String) {

  def withHost(host: String): TypesenseSettings = new TypesenseSettings(host, apiKey)

  def withApiKey(apiKey: String): TypesenseSettings = new TypesenseSettings(host, apiKey)

  override def equals(other: Any): Boolean = other match {
    case that: TypesenseSettings =>
      host == that.host &&
      apiKey == that.apiKey
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(host, apiKey)
}

object TypesenseSettings {
  def apply(host: String, apiKey: String): TypesenseSettings = new TypesenseSettings(host, apiKey)

  def create(host: String, apiKey: String): TypesenseSettings = new TypesenseSettings(host, apiKey)
}
