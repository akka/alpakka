/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.HttpEntity
import akka.stream.alpakka.azure.storage.impl.AzureStorageStream
import akka.stream.alpakka.azure.storage.requests.{
  CreateContainer,
  DeleteBlob,
  GetBlob,
  GetProperties,
  PutAppendBlock,
  PutBlockBlob,
  PutPageBlock
}
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
   * @param requestBuilder builder to build getBlob request
   * @return A [[akka.stream.scaladsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getBlob(objectPath: String, requestBuilder: GetBlob): Source[ByteString, Future[ObjectMetadata]] =
    AzureStorageStream.getObject(BlobType, objectPath, requestBuilder)

  /**
   * Gets blob properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder versionId of the blob (if applicable)
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String, requestBuilder: GetProperties): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.getBlobProperties(objectPath, requestBuilder)

  /**
   * Deletes blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build deleteBlob request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteBlob(objectPath: String, requestBuilder: DeleteBlob): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.deleteBlob(objectPath, requestBuilder)

  /**
   * Put Block blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build putBlockBlob request
   * @param payload actual payload, a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putBlockBlob(objectPath: String,
                   requestBuilder: PutBlockBlob,
                   payload: Source[ByteString, _]): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .putBlob(objectPath,
               requestBuilder,
               Some(HttpEntity(requestBuilder.contentType, requestBuilder.contentLength, payload)))

  /**
   * Put (Create) Page Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build putAppendBlob request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putPageBlock(objectPath: String, requestBuilder: PutPageBlock): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(objectPath, requestBuilder, None)

  /**
   * Put (Create) Append Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putAppendBlock(objectPath: String, requestBuilder: PutAppendBlock): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.putBlob(objectPath, requestBuilder, None)

  /**
   * Create container.
   *
   * @param objectPath name of the container
   * @param requestBuilder builder to build createContainer request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createContainer(objectPath: String, requestBuilder: CreateContainer): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.createContainer(objectPath, requestBuilder)
}
