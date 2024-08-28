/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package javadsl

import akka.NotUsed
import akka.http.javadsl.model._
import akka.http.javadsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.http.scaladsl.model.{HttpEntity, ContentType => ScalaContentType}
import akka.stream.alpakka.azure.storage.headers.BlobTypeHeader
import akka.stream.alpakka.azure.storage.impl.AzureStorageStream
import akka.stream.javadsl.Source
import akka.stream.scaladsl.SourceToCompletionStage
import akka.util.ByteString

import java.util.Optional
import java.util.concurrent.CompletionStage

/**
 * Java API for BlobService operations.
 */
object BlobService {

  /**
   * Gets blob representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param range range to download
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getBlob(objectPath: String,
              range: ByteRange,
              versionId: Optional[String],
              leaseId: Optional[String]): Source[ByteString, CompletionStage[ObjectMetadata]] =
    new Source(
      AzureStorageStream
        .getObject(
          BlobType,
          objectPath,
          Option(versionId.orElse(null)),
          StorageHeaders
            .create()
            .withRangeHeader(range.asInstanceOf[ScalaByteRange])
            .withLeaseIdHeader(Option(leaseId.orElse(null)))
            .headers
        )
        .toCompletionStage()
    )

  /**
   * Gets blob representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getBlob(objectPath: String,
              versionId: Optional[String],
              leaseId: Optional[String]): Source[ByteString, CompletionStage[ObjectMetadata]] =
    new Source(
      AzureStorageStream
        .getObject(
          BlobType,
          objectPath,
          Option(versionId.orElse(null)),
          StorageHeaders
            .create()
            .withLeaseIdHeader(Option(leaseId.orElse(null)))
            .headers
        )
        .toCompletionStage()
    )

  /**
   * Gets blob properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String,
                    versionId: Optional[String],
                    leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .getObjectProperties(BlobType,
                           objectPath,
                           Option(versionId.orElse(null)),
                           StorageHeaders.create().withLeaseIdHeader(Option(leaseId.orElse(null))).headers)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Deletes blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteBlob(objectPath: String,
                 versionId: Optional[String],
                 leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .deleteObject(BlobType,
                    objectPath,
                    Option(versionId.orElse(null)),
                    StorageHeaders.create().withLeaseIdHeader(Option(leaseId.orElse(null))).headers)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put Block blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param contentLength length of the blob
   * @param payload actual payload, a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putBlockBlob(objectPath: String,
                   contentType: ContentType,
                   contentLength: Long,
                   payload: Source[ByteString, _],
                   leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .putBlockBlob(
        objectPath,
        HttpEntity(contentType.asInstanceOf[ScalaContentType], contentLength, payload.asScala),
        StorageHeaders
          .create()
          .withContentLengthHeader(contentLength)
          .withContentTypeHeader(contentType.asInstanceOf[ScalaContentType])
          .withLeaseIdHeader(Option(leaseId.orElse(null)))
          .withBlobTypeHeader(BlobTypeHeader.BlockBlobHeader)
          .headers
      )
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put (Create) Page Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param maxBlockSize maximum block size
   * @param blobSequenceNumber optional block sequence number
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putPageBlock(objectPath: String,
                   contentType: ContentType,
                   maxBlockSize: Long,
                   blobSequenceNumber: Option[Int],
                   leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed.type] =
    AzureStorageStream
      .putPageOrAppendBlock(
        objectPath,
        StorageHeaders
          .create()
          .withContentLengthHeader(0L)
          .withContentTypeHeader(contentType.asInstanceOf[ScalaContentType])
          .withBlobTypeHeader(BlobTypeHeader.PageBlobHeader)
          .withPageBlobContentLengthHeader(maxBlockSize)
          .withPageBlobSequenceNumberHeader(blobSequenceNumber)
          .withLeaseIdHeader(Option(leaseId.orElse(null)))
          .headers
      )
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put (Create) Append Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putAppendBlock(objectPath: String,
                     contentType: ContentType,
                     leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed.type] =
    AzureStorageStream
      .putPageOrAppendBlock(
        objectPath,
        StorageHeaders
          .create()
          .withContentLengthHeader(0L)
          .withContentTypeHeader(contentType.asInstanceOf[ScalaContentType])
          .withBlobTypeHeader(BlobTypeHeader.AppendBlobHeader)
          .withLeaseIdHeader(Option(leaseId.orElse(null)))
          .headers
      )
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Create container.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createContainer(objectPath: String): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream.createContainer(objectPath).map(opt => Optional.ofNullable(opt.orNull)).asJava
}
