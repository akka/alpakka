/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpRequest, Uri}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption

abstract class RequestBuilder(val sse: Option[ServerSideEncryption] = None,
                              val additionalHeaders: Seq[HttpHeader] = Seq.empty) {

  protected val method: HttpMethod

  protected def queryParams: Map[String, String] = Map.empty

  def withServerSideEncryption(sse: ServerSideEncryption): RequestBuilder

  def addHeader(httpHeader: HttpHeader): RequestBuilder

  def addHeader(name: String, value: String): RequestBuilder = addHeader(RawHeader(name, value))

  protected def getHeaders: Seq[HttpHeader]

  private[storage] def createRequest(settings: StorageSettings, storageType: String, objectPath: String): HttpRequest =
    HttpRequest(
      method = method,
      uri = createUri(settings = settings,
                      storageType = storageType,
                      objectPath = objectPath,
                      queryString = createQueryString(settings, Some(Query(queryParams).toString()))),
      headers = getHeaders
    )

  private def createUri(settings: StorageSettings,
                        storageType: String,
                        objectPath: String,
                        queryString: Option[String]): Uri = {
    val accountName = settings.azureNameKeyCredential.accountName
    val path = if (objectPath.startsWith("/")) objectPath else s"/$objectPath"
    settings.endPointUrl
      .map { endPointUrl =>
        val qs = queryString.getOrElse("")
        Uri(endPointUrl).withPath(Uri.Path(s"/$accountName$path")).withQuery(Uri.Query(qs))
      }
      .getOrElse(
        Uri.from(
          scheme = "https",
          host = s"$accountName.$storageType.core.windows.net",
          path = Uri.Path(path).toString(),
          queryString = queryString
        )
      )
  }

  private def createQueryString(settings: StorageSettings, apiQueryString: Option[String]): Option[String] = {
    if (settings.authorizationType == SasAuthorizationType) {
      if (settings.sasToken.isEmpty) throw new RuntimeException("SAS token must be defined for SAS authorization type.")
      else {
        val sasToken = settings.sasToken.get
        Some(apiQueryString.map(qs => s"$sasToken&$qs").getOrElse(sasToken))
      }
    } else apiQueryString
  }
}
