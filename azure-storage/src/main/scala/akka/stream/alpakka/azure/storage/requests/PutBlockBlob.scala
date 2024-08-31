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

final class PutBlockBlob(val contentLength: Long,
                         val contentType: ContentType,
                         val leaseId: Option[String] = None,
                         override val sse: Option[ServerSideEncryption] = None,
                         override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  def withLeaseId(leaseId: String): PutBlockBlob = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): PutBlockBlob = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): PutBlockBlob =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(contentLength)
      .withContentTypeHeader(contentType)
      .witServerSideEncryption(sse)
      .withBlobTypeHeader(BlobTypeHeader.BlockBlobHeader)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutBlockBlob(contentLength = contentLength,
                     contentType = contentType,
                     leaseId = leaseId,
                     sse = sse,
                     additionalHeaders = additionalHeaders)
}

object PutBlockBlob {

  /*
   * Scala API
   */
  def apply(contentLength: Long, contentType: ContentType): PutBlockBlob = new PutBlockBlob(contentLength, contentType)

  /*
   * Java API
   */
  def create(contentLength: Long, contentType: JavaContentType): PutBlockBlob =
    PutBlockBlob(contentLength, contentType.asInstanceOf[ContentType])
}
