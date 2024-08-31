/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.javadsl.model.{ContentType => JavaContentType}
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{ContentType, HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.{RangeWriteTypeHeader, ServerSideEncryption}
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class UpdateFileRange(val range: ByteRange.Slice,
                            val contentType: ContentType,
                            val leaseId: Option[String] = None,
                            override val sse: Option[ServerSideEncryption] = None,
                            override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  override protected val queryParams: Map[String, String] = super.queryParams ++ Map("comp" -> "range")

  def withLeaseId(leaseId: String): UpdateFileRange = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): UpdateFileRange = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): UpdateFileRange =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new UpdateFileRange(contentType = contentType,
                        range = range,
                        leaseId = leaseId,
                        sse = sse,
                        additionalHeaders = additionalHeaders)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(range.last - range.first + 1)
      .withContentTypeHeader(contentType)
      .withRangeHeader(range)
      .withLeaseIdHeader(leaseId)
      .withRangeWriteTypeHeader(RangeWriteTypeHeader.UpdateFileHeader)
      .witServerSideEncryption(sse)
      .withAdditionalHeaders(additionalHeaders)
      .headers
}

object UpdateFileRange {

  /*
   * Scala API
   */
  def apply(range: ByteRange.Slice, contentType: ContentType): UpdateFileRange = new UpdateFileRange(range, contentType)

  /*
   * Java API
   */
  def create(range: ByteRange.Slice, contentType: JavaContentType): UpdateFileRange =
    UpdateFileRange(range, contentType.asInstanceOf[ContentType])
}
