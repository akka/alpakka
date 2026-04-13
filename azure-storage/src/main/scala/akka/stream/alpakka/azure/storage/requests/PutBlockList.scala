/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.annotation.InternalApi
import akka.http.scaladsl.model.{ContentTypes, HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

/**
 * INTERNAL API
 *
 * Request builder for the Put Block List operation. Commits a blob by specifying its block IDs.
 */
@InternalApi
private[storage] final class PutBlockList(val contentLength: Long,
                                          val leaseId: Option[String] = None,
                                          override val sse: Option[ServerSideEncryption] = None,
                                          override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  override protected val queryParams: Map[String, String] =
    Map("comp" -> "blocklist")

  override def withServerSideEncryption(sse: ServerSideEncryption): PutBlockList = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): PutBlockList =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(contentLength)
      .withContentTypeHeader(ContentTypes.`text/plain(UTF-8)`)
      .witServerSideEncryption(sse)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(sse: Option[ServerSideEncryption] = sse, additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutBlockList(contentLength = contentLength, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

/** INTERNAL API */
@InternalApi
private[storage] object PutBlockList {
  def apply(contentLength: Long): PutBlockList = new PutBlockList(contentLength)
}
