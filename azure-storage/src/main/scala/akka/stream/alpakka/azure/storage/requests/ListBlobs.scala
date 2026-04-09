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

import java.util.{Optional, OptionalInt}
import scala.jdk.OptionConverters._

final class ListBlobs(val prefix: Option[String] = None,
                      val delimiter: Option[String] = None,
                      val maxResults: Option[Int] = None,
                      private[storage] val marker: Option[String] = None,
                      override val sse: Option[ServerSideEncryption] = None,
                      override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.GET

  override protected val queryParams: Map[String, String] = super.queryParams ++
    Map("restype" -> "container", "comp" -> "list") ++
    prefix.map("prefix" -> _).toMap ++
    delimiter.map("delimiter" -> _).toMap ++
    maxResults.map("maxresults" -> _.toString).toMap ++
    marker.map("marker" -> _).toMap

  /** Java API */
  def getPrefix: Optional[String] = prefix.toJava

  /** Java API */
  def getDelimiter: Optional[String] = delimiter.toJava

  /** Java API */
  def getMaxResults: OptionalInt = maxResults.fold(OptionalInt.empty())(OptionalInt.of)

  def withPrefix(prefix: String): ListBlobs = copy(prefix = Some(prefix))

  def withDelimiter(delimiter: String): ListBlobs = copy(delimiter = Some(delimiter))

  def withMaxResults(maxResults: Int): ListBlobs = copy(maxResults = Some(maxResults))

  private[storage] def withMarker(marker: String): ListBlobs = copy(marker = Some(marker))

  override def withServerSideEncryption(sse: ServerSideEncryption): ListBlobs = copy(sse = Some(sse))

  override def addHeader(httpHeader: HttpHeader): ListBlobs =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(prefix: Option[String] = prefix,
                   delimiter: Option[String] = delimiter,
                   maxResults: Option[Int] = maxResults,
                   marker: Option[String] = marker,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new ListBlobs(prefix = prefix,
                  delimiter = delimiter,
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

object ListBlobs {

  /** Scala API */
  def apply(): ListBlobs = new ListBlobs()

  /** Java API */
  def create(): ListBlobs = new ListBlobs()
}
