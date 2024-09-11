/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class DeleteDirectory(override val sse: Option[ServerSideEncryption] = None,
                            override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.DELETE

  override protected val queryParams: Map[String, String] = super.queryParams ++ Map("restype" -> "directory")

  override def withServerSideEncryption(sse: ServerSideEncryption): DeleteDirectory = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): DeleteDirectory =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(sse: Option[ServerSideEncryption] = sse, additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new DeleteDirectory(sse = sse, additionalHeaders = additionalHeaders)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .witServerSideEncryption(sse)
      .withAdditionalHeaders(additionalHeaders)
      .headers
}

object DeleteDirectory {

  /*
   * Scala API
   */
  def apply(): DeleteDirectory = new DeleteDirectory()

  /*
   * Java API
   */
  def create(): DeleteDirectory = DeleteDirectory()
}
