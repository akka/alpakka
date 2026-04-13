/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.javadsl.model.{ContentType => JavaContentType}
import akka.http.scaladsl.model.{ContentType, HttpHeader}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption

/**
 * Request builder for streaming block blob uploads using the Put Block / Put Block List API.
 * Unlike [[PutBlockBlob]], this does not require knowing the content length upfront.
 *
 * @param contentType content type of the blob
 * @param blockSize size of each block in bytes (default 4 MB, max 100 MB with API version 2024-11-04).
 *                  Azure limits a block blob to 50,000 blocks, so the block size determines the maximum blob size.
 * @param leaseId optional lease ID
 * @param sse optional server-side encryption
 * @param additionalHeaders optional additional headers
 */
final class PutBlockBlobStreaming(val contentType: ContentType,
                                  val blockSize: Int = 4 * 1024 * 1024,
                                  val leaseId: Option[String] = None,
                                  val sse: Option[ServerSideEncryption] = None,
                                  val additionalHeaders: Seq[HttpHeader] = Seq.empty) {

  def withBlockSize(blockSize: Int): PutBlockBlobStreaming = copy(blockSize = blockSize)

  def withLeaseId(leaseId: String): PutBlockBlobStreaming = copy(leaseId = Option(leaseId))

  def withServerSideEncryption(sse: ServerSideEncryption): PutBlockBlobStreaming = copy(sse = Option(sse))

  def addHeader(httpHeader: HttpHeader): PutBlockBlobStreaming =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(blockSize: Int = blockSize,
                   leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new PutBlockBlobStreaming(contentType = contentType,
                              blockSize = blockSize,
                              leaseId = leaseId,
                              sse = sse,
                              additionalHeaders = additionalHeaders)
}

object PutBlockBlobStreaming {

  /*
   * Scala API
   */
  def apply(contentType: ContentType): PutBlockBlobStreaming = new PutBlockBlobStreaming(contentType)

  /*
   * Java API
   */
  def create(contentType: JavaContentType): PutBlockBlobStreaming =
    PutBlockBlobStreaming(contentType.asInstanceOf[ContentType])
}
