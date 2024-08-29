/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package headers

import akka.annotation.InternalApi
import akka.http.scaladsl.model.{ContentType, HttpHeader}
import akka.http.scaladsl.model.headers.{CustomHeader, RawHeader}

private[storage] case class CustomContentTypeHeader(contentType: ContentType) extends CustomHeader {
  override def name(): String = "Content-Type"

  override def value(): String = contentType.value

  override def renderInRequests(): Boolean = true

  override def renderInResponses(): Boolean = true
}

private[storage] case class CustomContentLengthHeader(contentLength: Long) extends CustomHeader {
  override def name(): String = "Content-Length"

  override def value(): String = contentLength.toString

  override def renderInRequests(): Boolean = true

  override def renderInResponses(): Boolean = true
}

private[storage] case class BlobTypeHeader(blobType: String) {
  @InternalApi private[storage] def header: HttpHeader = RawHeader(BlobTypeHeaderKey, blobType)
}

object BlobTypeHeader {
  private[storage] val BlockBlobHeader = new BlobTypeHeader(BlockBlobType)
  private[storage] val PageBlobHeader = new BlobTypeHeader(PageBlobType)
  private[storage] val AppendBlobHeader = new BlobTypeHeader(AppendBlobType)
}

private[storage] case class RangeWriteTypeHeader(headerName: String, writeType: String) {
  @InternalApi private[storage] def header: HttpHeader = RawHeader(headerName, writeType)
}

object RangeWriteTypeHeader {
  private[storage] val UpdateFileHeader = new RangeWriteTypeHeader(FileWriteTypeHeaderKey, "update")
  private[storage] val ClearFileHeader = new RangeWriteTypeHeader(FileWriteTypeHeaderKey, "clear")
  private[storage] val UpdatePageHeader = new RangeWriteTypeHeader(PageWriteTypeHeaderKey, "update")
  private[storage] val ClearPageHeader = new RangeWriteTypeHeader(PageWriteTypeHeaderKey, "clear")
}
