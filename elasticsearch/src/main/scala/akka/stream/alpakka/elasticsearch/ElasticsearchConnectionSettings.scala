/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

final class ElasticsearchConnectionSettings private (
    val baseUrl: String,
    val username: Option[String],
    val password: Option[String]
) {

  def withBaseUrl(value: String): ElasticsearchConnectionSettings = copy(baseUrl = value)

  def withUsername(value: String): ElasticsearchConnectionSettings = copy(username = Option(value))

  def withPassword(value: String): ElasticsearchConnectionSettings = copy(password = Option(value))

  def hasCredentialsDefined: Boolean = username.isDefined && password.isDefined

  def copy(baseUrl: String = baseUrl,
           username: Option[String] = username,
           password: Option[String] = password): ElasticsearchConnectionSettings =
    new ElasticsearchConnectionSettings(baseUrl = baseUrl, username = username, password = password)

  override def toString =
    s"""ElasticsearchSourceSettings(baseUrl=$baseUrl,username=$username,password=$password)"""
}

object ElasticsearchConnectionSettings {

  val Default = new ElasticsearchConnectionSettings(
    baseUrl = "",
    username = None,
    password = None
  )

  /** Scala API */
  def apply(): ElasticsearchConnectionSettings = Default

  /** Java API */
  def create(): ElasticsearchConnectionSettings = Default
}
