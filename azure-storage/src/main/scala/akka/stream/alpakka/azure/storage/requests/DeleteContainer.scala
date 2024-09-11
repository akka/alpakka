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

final class DeleteContainer(val leaseId: Option[String] = None,
                            override val sse: Option[ServerSideEncryption] = None,
                            override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.DELETE

  override protected val queryParams: Map[String, String] = super.queryParams ++ Map("restype" -> "container")

  def withLeaseId(leaseId: String): DeleteContainer = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): DeleteContainer = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): DeleteContainer =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .witServerSideEncryption(sse)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new DeleteContainer(leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

object DeleteContainer {

  /*
   * Scala API
   */
  def apply(): DeleteContainer = new DeleteContainer()

  /*
   * Java API
   */
  def create(): DeleteContainer = DeleteContainer()
}
