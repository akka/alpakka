/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package requests

import akka.http.javadsl.model.headers.{ByteRange => JavaByteRange}
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpMethods}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.impl.StorageHeaders

final class GetFile(val versionId: Option[String] = None,
                    val range: Option[ByteRange] = None,
                    val leaseId: Option[String] = None,
                    override val sse: Option[ServerSideEncryption] = None,
                    override val additionalHeaders: Seq[HttpHeader] = Seq.empty)
    extends RequestBuilder {

  override protected val method: HttpMethod = HttpMethods.GET

  override protected val queryParams: Map[String, String] = super.queryParams ++
    versionId.map(value => "versionId" -> value).toMap

  def withVersionId(versionId: String): GetFile = copy(versionId = Option(versionId))

  /** Java API */
  def withRange(range: JavaByteRange): GetFile = withRange(range.asInstanceOf[ByteRange])

  /** Scala API */
  def withRange(range: ByteRange): GetFile = copy(range = Option(range))

  def withLeaseId(leaseId: String): GetFile = copy(leaseId = Option(leaseId))

  override def withServerSideEncryption(sse: ServerSideEncryption): GetFile = copy(sse = Option(sse))

  override def addHeader(httpHeader: HttpHeader): GetFile =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(versionId: Option[String] = versionId,
                   range: Option[ByteRange] = range,
                   leaseId: Option[String] = leaseId,
                   sse: Option[ServerSideEncryption] = sse,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new GetFile(versionId = versionId,
                range = range,
                leaseId = leaseId,
                sse = sse,
                additionalHeaders = additionalHeaders)

  override protected def getHeaders: Seq[HttpHeader] =
    StorageHeaders()
      .witServerSideEncryption(sse)
      .withRangeHeader(range)
      .withLeaseIdHeader(leaseId)
      .withAdditionalHeaders(additionalHeaders)
      .headers
}

object GetFile {

  /*
   * Scala API
   */
  def apply(): GetFile = new GetFile()

  /*
   * Java API
   */
  def create(): GetFile = new GetFile()
}
