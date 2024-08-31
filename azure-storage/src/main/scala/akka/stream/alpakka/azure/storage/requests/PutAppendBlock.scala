/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.javadsl.model.{ContentType => JavaContentType}
import akka.http.scaladsl.model.{ContentType, HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.{BlobTypeHeader, ServerSideEncryption}
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class PutAppendBlock(val contentType: ContentType,
                           val leaseId: Option[String] = None,
                           override val sse: Option[ServerSideEncryption] = None,
                           override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  def withLeaseId(leaseId: String): PutAppendBlock = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): PutAppendBlock = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): PutAppendBlock =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(0L)
      .withContentTypeHeader(contentType)
      .witServerSideEncryption(sse)
      .withBlobTypeHeader(BlobTypeHeader.PageBlobHeader)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutAppendBlock(contentType = contentType, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

object PutAppendBlock {

  /*
   * Scala API
   */
  def apply(contentType: ContentType): PutAppendBlock = new PutAppendBlock(contentType)

  /*
   * Java API
   */
  def create(contentType: JavaContentType): PutAppendBlock = PutAppendBlock(contentType.asInstanceOf[ContentType])
}
