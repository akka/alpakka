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

final class DeleteBlob(val versionId: Option[String] = None,
                       val leaseId: Option[String] = None,
                       override val sse: Option[ServerSideEncryption] = None,
                       override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.DELETE

  override protected val queryParams: Map[String, String] = super.queryParams ++
    versionId.map(value => "versionId" -> value).toMap

  def withVersionId(versionId: String): DeleteBlob = copy(versionId = Option(versionId))

  def withLeaseId(leaseId: String): DeleteBlob = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): DeleteBlob = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): DeleteBlob =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .witServerSideEncryption(sse)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers

  private def copy(versionId: Option[String] = versionId,
                   leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new DeleteBlob(versionId = versionId, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

object DeleteBlob {

  /*
   * Scala API
   */
  def apply(): DeleteBlob = new DeleteBlob()

  /*
   * Java API
   */
  def create(): DeleteBlob = new DeleteBlob()
}
