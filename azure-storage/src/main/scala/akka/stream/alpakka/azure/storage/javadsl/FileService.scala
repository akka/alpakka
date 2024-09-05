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
  ClearFileRange,
  CreateFile,
  DeleteFile,
  GetFile,
  GetProperties,
  UpdateFileRange
}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.SourceToCompletionStage
import akka.util.ByteString

import java.util.Optional
import java.util.concurrent.CompletionStage

/**
 * Java API FileService operations
 */
object FileService {

  /**
   * Gets file representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build getBlob request
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getFile(objectPath: String, requestBuilder: GetFile): Source[ByteString, CompletionStage[ObjectMetadata]] = {
    new Source(
      AzureStorageStream
        .getObject(FileType, objectPath, requestBuilder)
        .toCompletionStage()
    )
  }

  /**
   * Gets file properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build getFile proroperties request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String, requestBuilder: GetProperties): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .getFileProperties(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Deletes file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build getFile proroperties request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteFile(objectPath: String, requestBuilder: DeleteFile): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .deleteFile(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Creates a file.
   *
   * @param objectPath  path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build createFile request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createFile(objectPath: String, requestBuilder: CreateFile): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .createFile(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Updates file on the specified range.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder range of bytes to be written
   * @param payload actual payload, a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def updateRange(objectPath: String,
                  requestBuilder: UpdateFileRange,
                  payload: Source[ByteString, _]): Source[Optional[ObjectMetadata], NotUsed] = {
    AzureStorageStream
      .updateRange(
        objectPath,
        HttpEntity(requestBuilder.contentType,
                   requestBuilder.range.last - requestBuilder.range.first + 1,
                   payload.asScala),
        requestBuilder
      )
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava
  }

  /**
   * Clears specified range from the file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build clearRange request
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def clearRange(objectPath: String, requestBuilder: ClearFileRange): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .clearRange(objectPath, requestBuilder)
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava
}
