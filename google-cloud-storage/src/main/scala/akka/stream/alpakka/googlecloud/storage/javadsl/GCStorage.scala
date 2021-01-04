/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.http.javadsl.model.ContentType
import akka.http.scaladsl.model.{ContentType => ScalaContentType}
import akka.stream.alpakka.googlecloud.storage.impl.GCStorageStream
import akka.stream.alpakka.googlecloud.storage.{Bucket, StorageObject}
import akka.stream.javadsl.{RunnableGraph, Sink, Source}
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

/**
 * Java API
 *
 * Factory of Google Cloud Storage operations.
 */
object GCStorage {

  /**
   * Gets information on a bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
   *
   * @param bucketName the name of the bucket to look up
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @return a `CompletionStage` containing `Bucket` if it exists
   */
  def getBucket(bucketName: String,
                materializer: Materializer,
                attributes: Attributes): CompletionStage[Optional[Bucket]] =
    GCStorageStream.getBucket(bucketName)(materializer, attributes).map(_.asJava)(materializer.executionContext).toJava

  /**
   * Gets information on a bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
   *
   * @param bucketName the name of the bucket to look up
   * @return a `Source` containing `Bucket` if it exists
   */
  def getBucketSource(bucketName: String): Source[Optional[Bucket], NotUsed] =
    GCStorageStream.getBucketSource(bucketName).map(_.asJava).asJava

  /**
   * Creates a new bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   *
   * @param bucketName the name of the bucket
   * @param location the region to put the bucket in
   * @return a `CompletionStage` of `Bucket` with created bucket
   */
  def createBucket(bucketName: String,
                   location: String,
                   materializer: Materializer,
                   attributes: Attributes): CompletionStage[Bucket] =
    GCStorageStream.createBucket(bucketName, location)(materializer, attributes).toJava

  /**
   * Creates a new bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   *
   * @param bucketName the name of the bucket
   * @param location the region to put the bucket in
   * @return a `Source` of `Bucket` with created bucket
   */
  def createBucketSource(bucketName: String, location: String): Source[Bucket, NotUsed] =
    GCStorageStream.createBucketSource(bucketName, location).asJava

  /**
   * Deletes bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/delete
   *
   * @param bucketName the name of the bucket
   * @return a `CompletionStage` of `Done` on successful deletion
   */
  def deleteBucket(bucketName: String, materializer: Materializer, attributes: Attributes): CompletionStage[Done] =
    GCStorageStream.deleteBucket(bucketName)(materializer, attributes).toJava

  /**
   * Deletes bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/delete
   *
   * @param bucketName the name of the bucket
   * @return a `Source` of `Done` on successful deletion
   */
  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    GCStorageStream.deleteBucketSource(bucketName).asJava

  /**
   * Get storage object
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   *
   * @param bucket the name of the bucket
   * @param objectName the name of the object
   * @return a `Source` containing `StorageObject` if it exists
   */
  def getObject(bucket: String, objectName: String): Source[Optional[StorageObject], NotUsed] =
    GCStorageStream.getObject(bucket, objectName).map(_.asJava).asJava

  /**
   * Get storage object
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   *
   * @param bucket the name of the bucket
   * @param objectName the name of the object
   * @param generation the generation of the object
   * @return a `Source` containing `StorageObject` if it exists
   */
  def getObject(bucket: String, objectName: String, generation: Long): Source[Optional[StorageObject], NotUsed] =
    GCStorageStream.getObject(bucket, objectName, Option(generation)).map(_.asJava).asJava

  /**
   * Deletes object in bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/delete
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object
   * @return a `Source` of `Boolean` with `true` if object is deleted, `false` if object that we want to deleted doesn't exist
   */
  def deleteObject(bucketName: String, objectName: String): Source[java.lang.Boolean, NotUsed] =
    GCStorageStream.deleteObjectSource(bucketName, objectName).map(boolean2Boolean).asJava

  /**
   * Deletes object in bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/delete
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object
   * @param generation the generation of the object
   * @return a `Source` of `Boolean` with `true` if object is deleted, `false` if object that we want to deleted doesn't exist
   */
  def deleteObject(bucketName: String, objectName: String, generation: Long): Source[java.lang.Boolean, NotUsed] =
    GCStorageStream.deleteObjectSource(bucketName, objectName, Option(generation)).map(boolean2Boolean).asJava

  /**
   * Lists the bucket contents
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
   *
   * @param bucket the bucket name
   * @return a `Source` of `StorageObject`
   */
  def listBucket(bucket: String): Source[StorageObject, NotUsed] =
    GCStorageStream.listBucket(bucket, None).asJava

  /**
   * Lists the bucket contents
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
   *
   * @param bucket the bucket name
   * @param prefix the bucket prefix
   * @return a `Source` of `StorageObject`
   */
  def listBucket(bucket: String, prefix: String): Source[StorageObject, NotUsed] =
    GCStorageStream.listBucket(bucket, Option(prefix)).asJava

  /**
   * Lists the bucket contents
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
   *
   * @param bucket the bucket name
   * @param prefix the bucket prefix
   * @param versions if `true` list both live and archived bucket contents
   * @return a `Source` of `StorageObject`
   */
  def listBucket(bucket: String, prefix: String, versions: Boolean): Source[StorageObject, NotUsed] =
    GCStorageStream.listBucket(bucket, Option(prefix), versions).asJava

  /**
   * Downloads object from bucket.
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   *
   * @param bucket the bucket name
   * @param objectName the bucket prefix
   * @return  The source will emit an empty [[scala.Option Option]] if an object can not be found.
   *         Otherwise [[scala.Option Option]] will contain a source of object's data.
   */
  def download(bucket: String, objectName: String): Source[Optional[Source[ByteString, NotUsed]], NotUsed] =
    GCStorageStream.download(bucket, objectName).map(_.map(_.asJava).asJava).asJava

  /**
   * Downloads object from bucket.
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   *
   * @param bucket the bucket name
   * @param objectName the bucket prefix
   * @param generation the generation of the object
   * @return  The source will emit an empty [[scala.Option Option]] if an object can not be found.
   *         Otherwise [[scala.Option Option]] will contain a source of object's data.
   */
  def download(bucket: String,
               objectName: String,
               generation: Long): Source[Optional[Source[ByteString, NotUsed]], NotUsed] =
    GCStorageStream.download(bucket, objectName, Option(generation)).map(_.map(_.asJava).asJava).asJava

  /**
   * Uploads object, use this for small files and `resumableUpload` for big ones
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload
   *
   * @param bucket the bucket name
   * @param objectName the object name
   * @param data a `Source` of `ByteString`
   * @param contentType  the number of bytes that will be uploaded (required!)
   * @return a `Source` containing the `StorageObject` of the uploaded object
   */
  def simpleUpload(bucket: String,
                   objectName: String,
                   data: Source[ByteString, _],
                   contentType: ContentType): Source[StorageObject, NotUsed] =
    GCStorageStream.putObject(bucket, objectName, data.asScala, contentType.asInstanceOf[ScalaContentType]).asJava

  /**
   * Uploads object by making multiple requests
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
   *
   * @param bucket the bucket name
   * @param objectName the object name
   * @param contentType `ContentType`
   * @param chunkSize the size of the request sent to google cloud storage in bytes, must be a multiple of 256KB
   * @param metadata custom metadata for the object
   * @return a `Sink` that accepts `ByteString`'s and materializes to a `Future` of `StorageObject`
   */
  def resumableUpload(bucket: String,
                      objectName: String,
                      contentType: ContentType,
                      chunkSize: java.lang.Integer,
                      metadata: java.util.Map[String, String]): Sink[ByteString, CompletionStage[StorageObject]] = {
    assert(
      (chunkSize >= (256 * 1024)) && (chunkSize % (256 * 1024) == 0),
      "Chunk size must be a multiple of 256KB"
    )

    GCStorageStream
      .resumableUpload(bucket,
                       objectName,
                       contentType.asInstanceOf[ScalaContentType],
                       chunkSize,
                       Some(metadata.asScala.toMap))
      .asJava
      .mapMaterializedValue(func(_.toJava))
  }

  /**
   * Uploads object by making multiple requests
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
   *
   * @param bucket the bucket name
   * @param objectName the object name
   * @param contentType `ContentType`
   * @param chunkSize the size of the request sent to google cloud storage in bytes, must be a multiple of 256KB
   * @return a `Sink` that accepts `ByteString`'s and materializes to a `Future` of `StorageObject`
   */
  def resumableUpload(bucket: String,
                      objectName: String,
                      contentType: ContentType,
                      chunkSize: java.lang.Integer): Sink[ByteString, CompletionStage[StorageObject]] = {
    assert(
      (chunkSize >= (256 * 1024)) && (chunkSize % (256 * 1024) == 0),
      "Chunk size must be a multiple of 256KB"
    )

    GCStorageStream
      .resumableUpload(bucket, objectName, contentType.asInstanceOf[ScalaContentType], chunkSize)
      .asJava
      .mapMaterializedValue(func(_.toJava))
  }

  /**
   * Uploads object by making multiple requests with default chunk size of 5MB
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
   *
   * @param bucket the bucket name
   * @param objectName the object name
   * @param contentType `ContentType`
   * @return a `Sink` that accepts `ByteString`'s and materializes to a `scala.concurrent.Future Future` of `StorageObject`
   */
  def resumableUpload(bucket: String,
                      objectName: String,
                      contentType: ContentType): Sink[ByteString, CompletionStage[StorageObject]] =
    GCStorageStream
      .resumableUpload(bucket, objectName, contentType.asInstanceOf[ScalaContentType])
      .asJava
      .mapMaterializedValue(func(_.toJava))

  /**
   * Rewrites object to wanted destination by making multiple requests.
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
   *
   * @param sourceBucket the source bucket
   * @param sourceObjectName the source object name
   * @param destinationBucket the destination bucket
   * @param destinationObjectName the destination bucket name
   * @return a runnable graph which upon materialization will return a `CompletionStage` containing the `StorageObject` with info about rewritten file
   */
  def rewrite(sourceBucket: String,
              sourceObjectName: String,
              destinationBucket: String,
              destinationObjectName: String): RunnableGraph[CompletionStage[StorageObject]] =
    RunnableGraph
      .fromGraph(
        GCStorageStream.rewrite(sourceBucket, sourceObjectName, destinationBucket, destinationObjectName)
      )
      .mapMaterializedValue(func(_.toJava))

  /**
   * Deletes folder and its content.
   *
   * @param bucket the bucket name
   * @return a `Source` of `java.lang.Boolean` with all `true` if everything is deleted
   */
  def deleteObjects(bucket: String): Source[java.lang.Boolean, NotUsed] =
    GCStorageStream.deleteObjectsByPrefixSource(bucket, None).map(boolean2Boolean).asJava

  /**
   * Deletes folder and its content.
   *
   * @param bucket the bucket name
   * @param prefix the object prefix
   * @return a `Source` of `java.lang.Boolean` with all `true` if everything is deleted
   */
  def deleteObjectsByPrefix(bucket: String, prefix: String): Source[java.lang.Boolean, NotUsed] =
    GCStorageStream.deleteObjectsByPrefixSource(bucket, Option(prefix)).map(boolean2Boolean).asJava

  private def func[T, R](f: T => R) = new akka.japi.function.Function[T, R] {
    override def apply(param: T): R = f(param)
  }
}
