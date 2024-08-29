/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.azure.storage.headers.BlobTypeHeader
import akka.stream.alpakka.azure.storage.impl.AzureStorageStream
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

/**
 * Scala API for BlobService operations.
 */
object BlobService {

  /**
   * Gets blob representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param range range to download
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getBlob(objectPath: String,
              range: Option[ByteRange] = None,
              versionId: Option[String] = None,
              leaseId: Option[String] = None): Source[ByteString, Future[ObjectMetadata]] =
    AzureStorageStream.getObject(BlobType,
                                 objectPath,
                                 versionId,
                                 StorageHeaders().withRangeHeader(range).withLeaseIdHeader(leaseId).headers)

  /**
   * Gets blob properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String,
                    versionId: Option[String] = None,
                    leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.getObjectProperties(BlobType,
                                           objectPath,
                                           versionId,
                                           StorageHeaders().withLeaseIdHeader(leaseId).headers)

  /**
   * Deletes blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param versionId versionId of the blob (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteBlob(objectPath: String,
                 versionId: Option[String] = None,
                 leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.deleteObject(BlobType,
                                    objectPath,
                                    versionId,
                                    StorageHeaders().withLeaseIdHeader(leaseId).headers)

  /**
   * Put Block blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param contentLength length of the blob
   * @param payload actual payload, a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putBlockBlob(objectPath: String,
                   contentType: ContentType = ContentTypes.`application/octet-stream`,
                   contentLength: Long,
                   payload: Source[ByteString, _],
                   leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(
      objectPath,
      Some(HttpEntity(contentType, contentLength, payload)),
      StorageHeaders()
        .withContentLengthHeader(contentLength)
        .withContentTypeHeader(contentType)
        .withBlobTypeHeader(BlobTypeHeader.BlockBlobHeader)
        .withLeaseIdHeader(leaseId)
        .headers
    )

  /**
   * Put (Create) Page Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param maxBlockSize maximum block size
   * @param blobSequenceNumber optional block sequence number
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putPageBlock(objectPath: String,
                   contentType: ContentType = ContentTypes.`application/octet-stream`,
                   maxBlockSize: Long,
                   blobSequenceNumber: Option[Int] = None,
                   leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(
      objectPath,
      None,
      StorageHeaders()
        .withContentTypeHeader(contentType)
        .withBlobTypeHeader(BlobTypeHeader.PageBlobHeader)
        .withPageBlobContentLengthHeader(maxBlockSize)
        .withPageBlobSequenceNumberHeader(blobSequenceNumber)
        .withLeaseIdHeader(leaseId)
        .headers
    )

  /**
   * Put (Create) Append Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putAppendBlock(objectPath: String,
                     contentType: ContentType = ContentTypes.`application/octet-stream`,
                     leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(
      objectPath,
      None,
      StorageHeaders()
        .withContentTypeHeader(contentType)
        .withBlobTypeHeader(BlobTypeHeader.AppendBlobHeader)
        .withLeaseIdHeader(leaseId)
        .headers
    )

  /**
   * Create container.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createContainer(objectPath: String): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.createContainer(objectPath)
}
