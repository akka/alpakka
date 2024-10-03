/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package impl

import akka.http.scaladsl.model.headers.{ByteRange, RawHeader, Range => RangeHeader}
import akka.http.scaladsl.model.{ContentType, HttpHeader}
import akka.stream.alpakka.azure.storage.headers._

import java.util.Objects

private[storage] class StorageHeaders private (val contentLengthHeader: Option[HttpHeader] = None,
                                               val contentTypeHeader: Option[HttpHeader] = None,
                                               val sse: Option[ServerSideEncryption] = None,
                                               val rangeHeader: Option[HttpHeader] = None,
                                               val blobTypeHeader: Option[HttpHeader] = None,
                                               val leaseIdHeader: Option[HttpHeader] = None,
                                               val fileWriteTypeHeader: Option[HttpHeader] = None,
                                               val rangeWriteTypeHeader: Option[HttpHeader] = None,
                                               val fileMaxContentLengthHeader: Option[HttpHeader] = None,
                                               val pageBlobContentLengthHeader: Option[HttpHeader] = None,
                                               val pageBlobSequenceNumberHeader: Option[HttpHeader] = None,
                                               val additionalHeaders: Seq[HttpHeader] = Seq.empty) {

  private[storage] def headers: Seq[HttpHeader] =
    (contentLengthHeader ++
    contentTypeHeader ++
    rangeHeader ++
    blobTypeHeader ++
    leaseIdHeader ++
    fileWriteTypeHeader ++
    rangeWriteTypeHeader ++
    fileMaxContentLengthHeader ++
    pageBlobContentLengthHeader ++
    pageBlobSequenceNumberHeader).toSeq ++ sse.map(_.headers).getOrElse(Seq.empty) ++ additionalHeaders

  private[storage] def withContentLengthHeader(contentLength: Long): StorageHeaders =
    copy(contentLengthHeader = Some(CustomContentLengthHeader(contentLength)))

  private[storage] def withContentTypeHeader(contentType: ContentType): StorageHeaders =
    copy(contentTypeHeader = Some(CustomContentTypeHeader(contentType)))

  private[storage] def witServerSideEncryption(sse: Option[ServerSideEncryption]): StorageHeaders =
    copy(sse = sse)

  private[storage] def withRangeHeader(range: ByteRange): StorageHeaders =
    copy(rangeHeader = Some(RangeHeader(range)))

  private[storage] def withRangeHeader(range: Option[ByteRange]): StorageHeaders =
    copy(rangeHeader = range.map(value => RangeHeader(value)))

  private[storage] def withBlobTypeHeader(blobTypeHeader: BlobTypeHeader): StorageHeaders =
    copy(blobTypeHeader = Some(blobTypeHeader.header))

  private[storage] def withLeaseIdHeader(leaseId: Option[String]): StorageHeaders =
    copy(leaseIdHeader = leaseId.map(value => RawHeader(LeaseIdHeaderKey, value)))

  private[storage] def withRangeWriteTypeHeader(fileWriteTypeHeader: RangeWriteTypeHeader): StorageHeaders =
    copy(rangeWriteTypeHeader = Some(fileWriteTypeHeader.header))

  private[storage] def withFileTypeHeader(): StorageHeaders =
    copy(fileTypeHeader = Some(RawHeader(FileTypeHeaderKey, "file")))

  private[storage] def withFileMaxContentLengthHeader(contentLength: Long): StorageHeaders =
    copy(fileMaxContentLengthHeader = Some(RawHeader(XMsContentLengthHeaderKey, contentLength.toString)))

  private[storage] def withPageBlobContentLengthHeader(contentLength: Long): StorageHeaders =
    copy(pageBlobContentLengthHeader = Some(RawHeader(PageBlobContentLengthHeaderKey, contentLength.toString)))

  private[storage] def withPageBlobSequenceNumberHeader(sequenceNumber: Option[Int]): StorageHeaders =
    copy(
      pageBlobSequenceNumberHeader =
        sequenceNumber.map(value => RawHeader(PageBlobSequenceNumberHeaderKey, value.toString))
    )

  private[storage] def withAdditionalHeaders(additionalHeaders: Seq[HttpHeader]) =
    copy(additionalHeaders = additionalHeaders)

  private[storage] def addHeader(httpHeader: HttpHeader): StorageHeaders =
    copy(additionalHeaders = additionalHeaders :+ httpHeader)

  private def copy(contentLengthHeader: Option[HttpHeader] = contentLengthHeader,
                   contentTypeHeader: Option[HttpHeader] = contentTypeHeader,
                   rangeHeader: Option[HttpHeader] = rangeHeader,
                   sse: Option[ServerSideEncryption] = sse,
                   blobTypeHeader: Option[HttpHeader] = blobTypeHeader,
                   leaseIdHeader: Option[HttpHeader] = leaseIdHeader,
                   rangeWriteTypeHeader: Option[HttpHeader] = fileWriteTypeHeader,
                   fileTypeHeader: Option[HttpHeader] = rangeWriteTypeHeader,
                   fileMaxContentLengthHeader: Option[HttpHeader] = fileMaxContentLengthHeader,
                   pageBlobContentLengthHeader: Option[HttpHeader] = pageBlobContentLengthHeader,
                   pageBlobSequenceNumberHeader: Option[HttpHeader] = pageBlobSequenceNumberHeader,
                   additionalHeaders: Seq[HttpHeader] = additionalHeaders) =
    new StorageHeaders(
      contentLengthHeader = contentLengthHeader,
      contentTypeHeader = contentTypeHeader,
      sse = sse,
      rangeHeader = rangeHeader,
      blobTypeHeader = blobTypeHeader,
      leaseIdHeader = leaseIdHeader,
      fileWriteTypeHeader = rangeWriteTypeHeader,
      rangeWriteTypeHeader = fileTypeHeader,
      fileMaxContentLengthHeader = fileMaxContentLengthHeader,
      pageBlobContentLengthHeader = pageBlobContentLengthHeader,
      pageBlobSequenceNumberHeader = pageBlobSequenceNumberHeader,
      additionalHeaders = additionalHeaders
    )

  override def toString: String =
    s"""StorageHeaders(
       |contentLengthHeader=${contentLengthHeader.map(_.value()).getOrElse("None")},
       | contentTypeHeader=${contentTypeHeader.map(_.value()).getOrElse("None")},
       | rangeHeader=${rangeHeader.map(_.value()).getOrElse("None")},
       | sse=${sse.toString},
       | blobTypeHeader=${blobTypeHeader.map(_.value()).getOrElse("None")},
       | leaseIdHeader=${leaseIdHeader.map(_.value()).getOrElse("None")},
       | fileWriteTypeHeader=${fileWriteTypeHeader.map(_.value()).getOrElse("None")},
       | fileTypeHeader=${rangeWriteTypeHeader.map(_.value()).getOrElse("None")},
       | fileMaxContentLengthHeader=${fileMaxContentLengthHeader.map(_.value()).getOrElse("None")},
       | pageBlobContentLengthHeader=${pageBlobContentLengthHeader.map(_.value()).getOrElse("None")},
       | pageBlobSequenceNumberHeader=${pageBlobSequenceNumberHeader.map(_.value()).getOrElse("None")},
       | additionalHeaders=${additionalHeaders.mkString("[", " ,", "]")}
       |)""".stripMargin.replaceAll(System.lineSeparator(), "")

  override def equals(obj: Any): Boolean =
    obj match {
      case other: StorageHeaders =>
        Objects.equals(contentLengthHeader, other.contentLengthHeader) &&
        Objects.equals(contentTypeHeader, other.contentTypeHeader) &&
        Objects.equals(sse, other.sse) &&
        Objects.equals(rangeHeader, other.rangeHeader) &&
        Objects.equals(blobTypeHeader, other.blobTypeHeader) &&
        Objects.equals(leaseIdHeader, other.leaseIdHeader) &&
        Objects.equals(fileWriteTypeHeader, other.fileWriteTypeHeader) &&
        Objects.equals(fileMaxContentLengthHeader, other.fileMaxContentLengthHeader) &&
        Objects.equals(pageBlobContentLengthHeader, other.pageBlobContentLengthHeader) &&
        Objects.equals(pageBlobSequenceNumberHeader, other.pageBlobSequenceNumberHeader) &&
        Objects.equals(additionalHeaders, other.additionalHeaders)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(
      contentLengthHeader,
      contentTypeHeader,
      sse,
      rangeHeader,
      blobTypeHeader,
      leaseIdHeader,
      fileWriteTypeHeader,
      rangeWriteTypeHeader,
      fileMaxContentLengthHeader,
      pageBlobContentLengthHeader,
      pageBlobSequenceNumberHeader,
      additionalHeaders
    )
}

private[storage] object StorageHeaders {
  private[storage] val Empty = new StorageHeaders()

  private[storage] def apply(): StorageHeaders = Empty

  /**
   * Java Api
   */
  def create(): StorageHeaders = Empty
}
