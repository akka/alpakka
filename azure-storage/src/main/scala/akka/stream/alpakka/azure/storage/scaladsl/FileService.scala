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
  ClearFileRange,
  CreateFile,
  DeleteFile,
  GetFile,
  GetProperties,
  UpdateFileRange
}
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

/**
 * Scala API for FileService operations.
 */
object FileService {

  /**
   * Gets file representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build getFile request
   * @return A [[akka.stream.scaladsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getFile(objectPath: String, requestBuilder: GetFile): Source[ByteString, Future[ObjectMetadata]] =
    AzureStorageStream.getObject(FileType, objectPath, requestBuilder)

  /**
   * Gets file properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build getFile properties request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String, requestBuilder: GetProperties): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.getObjectProperties(FileType, objectPath, requestBuilder)

  /**
   * Deletes file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build deleteFile request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteFile(objectPath: String, requestBuilder: DeleteFile): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.deleteObject(FileType, objectPath, requestBuilder)

  /**
   * Creates a file.
   *
   * @param objectPath  path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build createFile request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createFile(objectPath: String, requestBuilder: CreateFile): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.createFile(objectPath, requestBuilder)

  /**
   * Updates file on the specified range.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build updateRange request
   * @param payload actual payload, a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def updateRange(objectPath: String,
                  requestBuilder: UpdateFileRange,
                  payload: Source[ByteString, _]): Source[Option[ObjectMetadata], NotUsed] = {
    AzureStorageStream.updateRange(
      objectPath,
      HttpEntity(requestBuilder.contentType, requestBuilder.range.last - requestBuilder.range.first + 1, payload),
      requestBuilder
    )
  }

  /**
   * Clears specified range from the file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param requestBuilder builder to build clearRange request
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def clearRange(objectPath: String, requestBuilder: ClearFileRange): Source[Option[ObjectMetadata], NotUsed] =
    AzureStorageStream.clearRange(objectPath, requestBuilder)
}
