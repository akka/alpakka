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

final class PutPageBlock(val maxBlockSize: Long,
                         val contentType: ContentType,
                         val leaseId: Option[String] = None,
                         val blobSequenceNumber: Option[Int] = None,
                         override val sse: Option[ServerSideEncryption] = None,
                         override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  def withLeaseId(leaseId: String): PutPageBlock = copy(leaseId = Option(leaseId))

  def withBlobSequenceNumber(blobSequenceNumber: Int): PutPageBlock =
    copy(blobSequenceNumber = Option(blobSequenceNumber))

  override def withServerSideEncryption(sse: ServerSideEncryption): PutPageBlock = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): PutPageBlock =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentLengthHeader(0L)
      .withPageBlobContentLengthHeader(maxBlockSize)
      .withContentTypeHeader(contentType)
      .witServerSideEncryption(sse)
      .withBlobTypeHeader(BlobTypeHeader.PageBlobHeader)
      .withLeaseIdHeader(leaseId)
      .withPageBlobSequenceNumberHeader(blobSequenceNumber)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(leaseId: Option[String] = leaseId,
                   blobSequenceNumber: Option[Int] = blobSequenceNumber,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutPageBlock(maxBlockSize = maxBlockSize,
                     contentType = contentType,
                     leaseId = leaseId,
                     blobSequenceNumber = blobSequenceNumber,
                     sse = sse,
                     additionalHeaders = additionalHeaders)
}

object PutPageBlock {

  /*
   * Scala API
   */
  def apply(maxBlockSize: Long, contentType: ContentType): PutPageBlock = new PutPageBlock(maxBlockSize, contentType)

  /*
   * Java API
   */
  def create(maxBlockSize: Long, contentType: JavaContentType): PutPageBlock =
    new PutPageBlock(maxBlockSize, contentType.asInstanceOf[ContentType])
}
