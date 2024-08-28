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
import akka.http.scaladsl.model.headers.ByteRange.Slice
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.http.scaladsl.model.{ContentType => ScalaContentType}
import akka.stream.alpakka.azure.storage.impl.AzureStorageStream
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
   * @param range range to download
   * @param versionId versionId of the file (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getFile(objectPath: String,
              range: ByteRange,
              versionId: Optional[String],
              leaseId: Optional[String]): Source[ByteString, CompletionStage[ObjectMetadata]] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    new Source(
      AzureStorageStream
        .getObject(FileType, objectPath, Some(scalaRange), Option(versionId.orElse(null)), Option(leaseId.orElse(null)))
        .toCompletionStage()
    )
  }

  /**
   * Gets file representing `objectPath` with specified range (if applicable).
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param versionId versionId of the file (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a
   *         materialized value containing the [[akka.stream.alpakka.azure.storage.ObjectMetadata]]
   */
  def getFile(objectPath: String,
              versionId: Optional[String],
              leaseId: Optional[String]): Source[ByteString, CompletionStage[ObjectMetadata]] = {
    new Source(
      AzureStorageStream
        .getObject(FileType, objectPath, None, Option(versionId.orElse(null)), Option(leaseId.orElse(null)))
        .toCompletionStage()
    )
  }

  /**
   * Gets file properties.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param versionId versionId of the file (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def getProperties(objectPath: String,
                    versionId: Optional[String],
                    leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .getObjectProperties(FileType, objectPath, Option(versionId.orElse(null)), Option(leaseId.orElse(null)))
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Deletes file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param versionId versionId of the file (if applicable)
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def deleteFile(objectPath: String,
                 versionId: Optional[String],
                 leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .deleteObject(FileType, objectPath, Option(versionId.orElse(null)), Option(leaseId.orElse(null)))
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Creates a file.
   *
   * @param objectPath  path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param contentType content type of the blob
   * @param maxSize maximum size of the file
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def createFile(objectPath: String,
                 contentType: ContentType,
                 maxSize: Long,
                 leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .createFile(objectPath, contentType.asInstanceOf[ScalaContentType], maxSize, Option(leaseId.orElse(null)))
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Updates file on the specified range.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param contentType content type of the blob
   * @param range range of bytes to be written
   * @param payload actual payload, a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def updateRange(objectPath: String,
                  contentType: ContentType,
                  range: Slice,
                  payload: Source[ByteString, _],
                  leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .updateOrClearRange(objectPath,
                          contentType.asInstanceOf[ScalaContentType],
                          range,
                          Some(payload.asScala),
                          Option(leaseId.orElse(null)))
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava

  /**
   * Clears specified range from the file.
   *
   * @param objectPath path of the object, should start with "/" and separated by `/`, e.g. `/share/my-directory/blob`
   * @param range range of bytes to be cleared
   * @param leaseId lease ID of an active lease (if applicable)
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[scala.Option]] of
   *         [[akka.stream.alpakka.azure.storage.ObjectMetadata]], will be [[scala.None]] in case the object does not exist
   */
  def clearRange(objectPath: String,
                 range: Slice,
                 leaseId: Optional[String]): Source[Optional[ObjectMetadata], NotUsed] =
    AzureStorageStream
      .updateOrClearRange(objectPath,
                          ContentTypes.NO_CONTENT_TYPE.asInstanceOf[ScalaContentType],
                          range,
                          None,
                          Option(leaseId.orElse(null)))
      .map(opt => Optional.ofNullable(opt.orNull))
      .asJava
}
