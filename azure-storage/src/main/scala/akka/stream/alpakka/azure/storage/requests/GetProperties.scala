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

final class GetProperties(val versionId: Option[String] = None,
                          val leaseId: Option[String] = None,
                          override val sse: Option[ServerSideEncryption] = None,
                          override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.HEAD

  override protected val queryParams: Map[String, String] = super.queryParams ++
    versionId.map(value => "versionId" -> value).toMap

  def withVersionId(versionId: String): GetProperties = copy(versionId = Option(versionId))

  def withLeaseId(leaseId: String): GetProperties = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): GetProperties = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): GetProperties =
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
    new GetProperties(versionId = versionId, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

object GetProperties {

  /*
   * Scala API
   */
  def apply(): GetProperties = new GetProperties()

  /*
   * Java API
   */
  def create(): GetProperties = new GetProperties()
}
