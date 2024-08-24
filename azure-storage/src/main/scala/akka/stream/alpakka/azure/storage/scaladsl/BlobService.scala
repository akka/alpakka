/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.{ContentType, ContentTypes}
import akka.http.scaladsl.model.headers.ByteRange
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
    AzureStorageStream.getObject(BlobType, objectPath, range, versionId, leaseId)

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
    AzureStorageStream.getObjectProperties(BlobType, objectPath, versionId, leaseId)

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
    AzureStorageStream.deleteObject(BlobType, objectPath, versionId, leaseId)

  /**
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param contentType content type of the blob
   * @param contentLength length of the blob
   * @param payload actual payload, a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param blobType type of the blob, ''Must be one of:'' __'''BlockBlob, PageBlob, or AppendBlob'''__
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putBlob(objectPath: String,
              contentType: ContentType = ContentTypes.`application/octet-stream`,
              contentLength: Long,
              payload: Source[ByteString, _],
              blobType: String = "BlockBlob",
              leaseId: Option[String] = None): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(blobType, objectPath, contentType, contentLength, payload, leaseId)
}
