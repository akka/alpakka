/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.elasticsearch

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpHeader.ParsingResult

import scala.jdk.CollectionConverters._
import javax.net.ssl.SSLContext

final class ElasticsearchConnectionSettings private (
    val baseUrl: String,
    val username: Option[String],
    val password: Option[String],
    val headers: List[HttpHeader],
    val connectionContext: Option[HttpsConnectionContext]
) {

  def withBaseUrl(value: String): ElasticsearchConnectionSettings = copy(baseUrl = value)

  def withCredentials(username: String, password: String): ElasticsearchConnectionSettings =
    copy(username = Option(username), password = Option(password))

  def hasCredentialsDefined: Boolean = username.isDefined && password.isDefined

  /** Scala API */
  def withHeaders(headers: List[HttpHeader]): ElasticsearchConnectionSettings =
    copy(headers = headers)

  /** Java API */
  def withHeaders(headers: java.util.List[akka.http.javadsl.model.HttpHeader]): ElasticsearchConnectionSettings = {
    val scalaHeaders = headers.asScala
      .map(x => {
        HttpHeader.parse(x.name(), x.value()) match {
          case ParsingResult.Ok(header, _) => header
          case ParsingResult.Error(error) =>
            throw new Exception(s"Unable to convert java HttpHeader to scala HttpHeader: ${error.summary}")
        }
      })
      .toList

    copy(headers = scalaHeaders)
  }

  def withSSLContext(
      sslContext: SSLContext
  ): ElasticsearchConnectionSettings = {
    copy(connectionContext = Option(ConnectionContext.httpsClient(sslContext)))
  }

  def hasConnectionContextDefined: Boolean = connectionContext.isDefined

  private def copy(
      baseUrl: String = baseUrl,
      username: Option[String] = username,
      password: Option[String] = password,
      headers: List[HttpHeader] = headers,
      connectionContext: Option[HttpsConnectionContext] = connectionContext
  ): ElasticsearchConnectionSettings =
    new ElasticsearchConnectionSettings(baseUrl = baseUrl,
                                        username = username,
                                        password = password,
                                        headers = headers,
                                        connectionContext = connectionContext)

  override def toString =
    s"""ElasticsearchConnectionSettings(baseUrl=$baseUrl,username=$username,password=${password.fold("")(
      _ => "***"
    )},headers=${headers.mkString(";")},connectionContext=$connectionContext)"""
}

object ElasticsearchConnectionSettings {

  /** Scala API */
  def apply(baseUrl: String): ElasticsearchConnectionSettings =
    new ElasticsearchConnectionSettings(baseUrl, None, None, List.empty, None)

  /** Java API */
  def create(baseUrl: String): ElasticsearchConnectionSettings =
    new ElasticsearchConnectionSettings(baseUrl, None, None, List.empty, None)
}
