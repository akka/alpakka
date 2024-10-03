/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.javadsl.model.{ContentType => JavaContentType}
import akka.http.scaladsl.model.{ContentType, HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class CreateFile(val maxFileSize: Long,
                       val contentType: ContentType,
                       val leaseId: Option[String] = None,
                       override val sse: Option[ServerSideEncryption] = None,
                       override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.PUT

  def withLeaseId(leaseId: String): CreateFile = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): CreateFile = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): CreateFile =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .withContentTypeHeader(contentType)
      .withFileMaxContentLengthHeader(maxFileSize)
      .withFileTypeHeader()
      .withLeaseIdHeader(leaseId)
      .witServerSideEncryption(sse)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new CreateFile(maxFileSize = maxFileSize,
                   contentType = contentType,
                   leaseId = leaseId,
                   sse = sse,
                   additionalHeaders = additionalHeaders)
}

object CreateFile {

  /*
   * Scala API
   */
  def apply(maxFileSize: Long, contentType: ContentType): CreateFile = new CreateFile(maxFileSize, contentType)

  /*
   * Java API
   */
  def create(maxFileSize: Long, contentType: JavaContentType): CreateFile =
    CreateFile(maxFileSize, contentType.asInstanceOf[ContentType])
}
