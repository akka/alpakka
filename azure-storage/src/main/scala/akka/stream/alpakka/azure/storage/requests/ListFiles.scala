/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class ListFiles(val prefix: Option[String] = None,
                      val maxResults: Option[Int] = None,
                      private[storage] val marker: Option[String] = None,
                      override val sse: Option[ServerSideEncryption] = None,
                      override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.GET

  override protected val queryParams: Map[String, String] = super.queryParams ++
    Map("restype" -> "directory", "comp" -> "list") ++
    prefix.map("prefix" -> _).toMap ++
    maxResults.map("maxresults" -> _.toString).toMap ++
    marker.map("marker" -> _).toMap

  def withPrefix(prefix: String): ListFiles = copy(prefix = Some(prefix))

  def withMaxResults(maxResults: Int): ListFiles = copy(maxResults = Some(maxResults))

  private[storage] def withMarker(marker: String): ListFiles = copy(marker = Some(marker))

  override def withServerSideEncryption(sse: ServerSideEncryption): ListFiles = copy(sse = Some(sse))

  override def addHeader(httpHeader: HttpHeader): ListFiles =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(prefix: Option[String] = prefix,
                   maxResults: Option[Int] = maxResults,
                   marker: Option[String] = marker,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new ListFiles(prefix = prefix,
                  maxResults = maxResults,
                  marker = marker,
                  sse = sse,
                  additionalHeaders = additionalHeaders)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .witServerSideEncryption(sse)
      .withAdditionalHeaders(additionalHeaders)
      .headers
}

object ListFiles {

  /** Scala API */
  def apply(): ListFiles = new ListFiles()

  /** Java API */
  def create(): ListFiles = new ListFiles()
}
