/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package javadsl

import akka.NotUsed
import akka.http.scaladsl.model.HttpEntity
import akka.stream.alpakka.azure.storage.impl.AzureStorageStream
import akka.stream.alpakka.azure.storage.requests.{
  CreateContainer,
  DeleteFile,
  GetBlob,
  GetProperties,
  PutAppendBlock,
  PutBlockBlob,
  PutPageBlock
}
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
   * @param requestBuilder builder to build getBlob properties request
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getBlob(objectPath: String, requestBuilder: GetBlob): Source[ByteString, CompletionStage[ObjectMetadata]] =
    new Source(
      AzureStorageStream
        .getObject(BlobType, objectPath, requestBuilder)
        .toCompletionStage()
    )

  /**
   * Gets blob properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build getBlob properties request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String, requestBuilder: GetProperties): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .getBlobProperties(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Deletes blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build deleteFile request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteBlob(objectPath: String, requestBuilder: DeleteFile): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .deleteBlob(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put Block blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build putBlockBlob request
   * @param payload actual payload, a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putBlockBlob(objectPath: String,
                   requestBuilder: PutBlockBlob,
                   payload: Source[ByteString, _]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .putBlob(
        objectPath,
        requestBuilder,
        Some(HttpEntity(requestBuilder.contentType, requestBuilder.contentLength, payload.asScala))
      )
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put (Create) Page Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build putPageBlob request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putPageBlock(objectPath: String, requestBuilder: PutPageBlock): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .putBlob(objectPath, requestBuilder, None)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Put (Create) Append Blob.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/container/blob`
   * @param requestBuilder builder to build putAppendBlob request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def putAppendBlock(objectPath: String, requestBuilder: PutAppendBlock): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .putBlob(objectPath, requestBuilder, None)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Create container.
   *
   * @param objectPath name of the container
   * @param requestBuilder builder to build createContainer request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createContainer(objectPath: String, requestBuilder: CreateContainer): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream.createContainer(objectPath, requestBuilder).map(opt => Optional.ofNullable(opt.orNull)).asJava
}
