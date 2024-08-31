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

final class DeleteFile(val versionId: Option[String] = None,
                       val leaseId: Option[String] = None,
                       override val sse: Option[ServerSideEncryption] = None,
                       override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.DELETE

  override protected val queryParams: Map[String, String] = super.queryParams ++
    versionId.map(value => "versionId" -> value).toMap

  def withVersionId(versionId: String): DeleteFile = copy(versionId = Option(versionId))

  def withLeaseId(leaseId: String): DeleteFile = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): DeleteFile = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): DeleteFile =
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
    new DeleteFile(versionId = versionId, leaseId = leaseId, sse = sse, additionalHeaders = additionalHeaders)
}

object DeleteFile {

  /*
   * Scala API
   */
  def apply(): DeleteFile = new DeleteFile()

  /*
   * Java API
   */
  def create(): DeleteFile = new DeleteFile()
}
