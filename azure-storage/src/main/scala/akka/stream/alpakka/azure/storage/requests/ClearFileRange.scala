/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.{RangeWriteTypeHeader, ServerSideEncryption}
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class ClearFileRange(val range: ByteRange.Slice,
                           val leaseId: Option[String] = None,
                           override val sse: Option[ServerSideEncryption] = None,
                           override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  override protected val queryParams: Map[String, String] = super.queryParams ++ Map("comp" -> "range")

  def withLeaseId(leaseId: String): ClearFileRange = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): ClearFileRange = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): ClearFileRange =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new ClearFileRange(range = range, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(0L)
      .withRangeHeader(range)
      .withLeaseIdHeader(leaseId)
      .withRangeWriteTypeHeader(RangeWriteTypeHeader.ClearFileHeader)
      .witServerSideEncryption(sse)
      .withAdditionalHeaders(additionalHeaders)
      .headers
}

object ClearFileRange {

  /*
   * Scala API
   */
  def apply(range: ByteRange.Slice): ClearFileRange = new ClearFileRange(range)

  /*
   * Java API
   */
  def create(range: ByteRange.Slice): ClearFileRange = ClearFileRange(range)
}
