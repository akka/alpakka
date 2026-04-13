/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.annotation.InternalApi
import akka.http.scaladsl.model.{ContentType, HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

/**
 * INTERNAL API
 *
 * Request builder for the Put Block operation. Uploads a single block as part of a block blob.
 */
@InternalApi
private[storage] final class PutBlock(val blockId: String,
                                      val contentLength: Long,
                                      val contentType: ContentType,
                                      val leaseId: Option[String] = None,
                                      override val sse: Option[ServerSideEncryption] = None,
                                      override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  override protected val queryParams: Map[String, String] =
    Map("comp" -> "block", "blockid" -> blockId)

  override def withServerSideEncryption(sse: ServerSideEncryption): PutBlock = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): PutBlock =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(contentLength)
      .withContentTypeHeader(contentType)
      .witServerSideEncryption(sse)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(sse: Option[ServerSideEncryption] = sse, additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutBlock(blockId = blockId,
                 contentLength = contentLength,
                 contentType = contentType,
                 leaseId = leaseId,
                 sse = sse,
                 additionalHeaders = additionalHeaders)
}

/** INTERNAL API */
@InternalApi
private[storage] object PutBlock {
  def apply(blockId: String, contentLength: Long, contentType: ContentType): PutBlock =
    new PutBlock(blockId, contentLength, contentType)
}
