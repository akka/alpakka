/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.scaladsl

import akka.http.scaladsl.model.ContentType
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.googlecloud.storage.impl.GCStorageStream
import akka.stream.alpakka.googlecloud.storage.{Bucket, StorageObject}
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future

/**
 * Factory of Google Cloud Storage operations.
 */
object GCStorage {

  /**
   * Gets information on a bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
   *
   * @param bucketName the name of the bucket to look up
   * @return a `Future` containing `Bucket` if it exists
   */
  def getBucket(bucketName: String)(implicit mat: Materializer,
                                    attr: Attributes = Attributes()): Future[Option[Bucket]] =
    GCStorageStream.getBucket(bucketName)

  /**
   * Gets information on a bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
   *
   * @param bucketName the name of the bucket to look up
   * @return a `Source` containing `Bucket` if it exists
   */
  def getBucketSource(bucketName: String): Source[Option[Bucket], NotUsed] =
    GCStorageStream.getBucketSource(bucketName)

  /**
   * Creates a new bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   *
   * @param bucketName the name of the bucket
   * @param location the region to put the bucket in
   * @return a `Future` of `Bucket` with created bucket
   */
  def createBucket(bucketName: String, location: String)(implicit mat: Materializer,
                                                         attr: Attributes = Attributes()): Future[Bucket] =
    GCStorageStream.createBucket(bucketName, location)

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
    GCStorageStream.createBucketSource(bucketName, location)

  /**
   * Deletes bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/delete
   *
   * @param bucketName the name of the bucket
   * @return a `Future` of `Done` on successful deletion
   */
  def deleteBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes = Attributes()): Future[Done] =
    GCStorageStream.deleteBucket(bucketName)

  /**
   * Deletes bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/delete
   *
   * @param bucketName the name of the bucket
   * @return a `Source` of `Done` on successful deletion
   */
  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    GCStorageStream.deleteBucketSource(bucketName)

  /**
   * Get storage object
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   *
   * @param bucket the name of the bucket
   * @param objectName the name of the object
   * @return a `Source` containing `StorageObject` if it exists
   */
  def getObject(bucket: String, objectName: String): Source[Option[StorageObject], NotUsed] =
    GCStorageStream.getObject(bucket, objectName)

  /**
   * Deletes object in bucket
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/delete
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object
   * @return a `Source` of `Boolean` with `true` if object is deleted, `false` if object that we want to deleted doesn't exist
   */
  def deleteObject(bucketName: String, objectName: String): Source[Boolean, NotUsed] =
    GCStorageStream.deleteObjectSource(bucketName, objectName)

  /**
   * Lists the bucket contents
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
   *
   * @param bucket the bucket name
   * @param prefix the bucket prefix
   * @return a `Source` of `StorageObject`
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[StorageObject, NotUsed] =
    GCStorageStream.listBucket(bucket, prefix)

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
  def download(bucket: String, objectName: String): Source[Option[Source[ByteString, NotUsed]], NotUsed] =
    GCStorageStream.download(bucket, objectName)

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
    GCStorageStream.putObject(bucket, objectName, data, contentType)

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
                      chunkSize: Int): Sink[ByteString, Future[StorageObject]] = {
    assert(
      (chunkSize >= (256 * 1024)) && (chunkSize % (256 * 1024) == 0),
      "Chunk size must be a multiple of 256KB"
    )
    GCStorageStream.resumableUpload(bucket, objectName, contentType, chunkSize)
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
                      contentType: ContentType): Sink[ByteString, Future[StorageObject]] =
    GCStorageStream.resumableUpload(bucket, objectName, contentType)

  /**
   * Rewrites object to wanted destination by making multiple requests.
   *
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
   *
   * @param sourceBucket the source bucket
   * @param sourceObjectName the source object name
   * @param destinationBucket the destination bucket
   * @param destinationObjectName the destination bucket name
   * @return a runnable graph which upon materialization will return a [[scala.concurrent.Future Future ]] containing the `StorageObject` with info about rewritten file
   */
  def rewrite(sourceBucket: String,
              sourceObjectName: String,
              destinationBucket: String,
              destinationObjectName: String): RunnableGraph[Future[StorageObject]] =
    GCStorageStream.rewrite(sourceBucket, sourceObjectName, destinationBucket, destinationObjectName)

  /**
   * Deletes folder and its content.
   *
   * @param bucket the bucket name
   * @param prefix the object prefix
   * @return a `Source` of `Boolean` with all `true` if everything is deleted
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String]): Source[Boolean, NotUsed] =
    GCStorageStream.deleteObjectsByPrefixSource(bucket, prefix)
}
