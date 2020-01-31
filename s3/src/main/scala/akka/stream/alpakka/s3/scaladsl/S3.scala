/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl
import akka.{Done, NotUsed}
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model._
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.impl._
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

/**
 * Factory of S3 operations.
 */
object S3 {
  val MinChunkSize: Int = 5242880

  /**
   * Use this for a low level access to S3.
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[akka.http.scaladsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return a raw HTTP response from S3
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod = HttpMethods.GET,
              versionId: Option[String] = None,
              s3Headers: S3Headers = S3Headers.empty): Source[HttpResponse, NotUsed] =
    S3Stream.request(S3Location(bucket, key), method, versionId = versionId, s3Headers = s3Headers.headers)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] that will be [[scala.None]] in case the object does not exist
   */
  def getObjectMetadata(
      bucket: String,
      key: String,
      versionId: Option[String] = None,
      sse: Option[ServerSideEncryption] = None
  ): Source[Option[ObjectMetadata], NotUsed] =
    getObjectMetadata(bucket, key, versionId, S3Headers.empty.withOptionalServerSideEncryption(sse))

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source Source]] containing an [[scala.Option]] that will be [[scala.None]] in case the object does not exist
   */
  def getObjectMetadata(
      bucket: String,
      key: String,
      versionId: Option[String],
      s3Headers: S3Headers
  ): Source[Option[ObjectMetadata], NotUsed] =
    S3Stream.getObjectMetadata(bucket, key, versionId, s3Headers)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObject(bucket: String, key: String, versionId: Option[String] = None): Source[Done, NotUsed] =
    deleteObject(bucket, key, versionId, S3Headers.empty)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObject(bucket: String,
                   key: String,
                   versionId: Option[String],
                   s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteObject(S3Location(bucket, key), versionId, s3Headers)

  /**
   * Deletes a S3 Objects which contain given prefix
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String]): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, prefix, S3Headers.empty)

  /**
   * Deletes a S3 Objects which contain given prefix
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String], s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteObjectsByPrefix(bucket, prefix, s3Headers)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Source Source]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType = ContentTypes.`application/octet-stream`,
                s3Headers: S3Headers): Source[ObjectMetadata, NotUsed] =
    S3Stream.putObject(S3Location(bucket, key), contentType, data, contentLength, s3Headers)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse [optional] the server side encryption used on upload
   * @return The source will emit an empty [[scala.Option Option]] if an object can not be found.
   *         Otherwise [[scala.Option Option]] will contain a tuple of object's data and metadata.
   */
  def download(
      bucket: String,
      key: String,
      range: Option[ByteRange] = None,
      versionId: Option[String] = None,
      sse: Option[ServerSideEncryption] = None
  ): Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] =
    download(bucket, key, range, versionId, S3Headers.empty.withOptionalServerSideEncryption(sse))

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param s3Headers any headers you want to add
   * @return The source will emit an empty [[scala.Option Option]] if an object can not be found.
   *         Otherwise [[scala.Option Option]] will contain a tuple of object's data and metadata.
   */
  def download(
      bucket: String,
      key: String,
      range: Option[ByteRange],
      versionId: Option[String],
      s3Headers: S3Headers
  ): Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] =
    S3Stream.download(S3Location(bucket, key), range, versionId, s3Headers)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html  (version 2 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html (version 1 API)
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    listBucket(bucket, prefix, S3Headers.empty)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html  (version 2 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html (version 1 API)
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String,
                 prefix: Option[String],
                 s3Headers: S3Headers): Source[ListBucketResultContents, NotUsed] =
    S3Stream.listBucket(bucket, prefix, s3Headers)

  /**
   * Will return a source of object metadata for a given bucket and delimiter with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html  (version 2 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html (version 1 API)
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String,
                 delimiter: String,
                 prefix: Option[String] = None,
                 s3Headers: S3Headers = S3Headers.empty): Source[ListBucketResultContents, NotUsed] =
    S3Stream
      .listBucketAndCommonPrefixes(bucket, delimiter, prefix, s3Headers)
      .mapConcat(_._1)

  /**
   * Will return a source of object metadata and common prefixes for a given bucket and delimiter with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html  (version 2 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html (prefix and delimiter documentation)
   * @param bucket    Which bucket that you list object metadata for
   * @param delimiter Delimiter to use for listing only one level of hierarchy
   * @param prefix    Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListBucketResultContents ListBucketResultContents]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListBucketResultContents ListBucketResultContents]])
   */
  def listBucketAndCommonPrefixes(
      bucket: String,
      delimiter: String,
      prefix: Option[String] = None,
      s3Headers: S3Headers = S3Headers.empty
  ): Source[(Seq[ListBucketResultContents], Seq[ListBucketResultCommonPrefixes]), NotUsed] =
    S3Stream.listBucketAndCommonPrefixes(bucket, delimiter, prefix, s3Headers)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param maxRetriesPerChunk the maximum number of times a given chunk upload will be retried on transient errors
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUpload(
      bucket: String,
      key: String,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      metaHeaders: MetaHeaders = MetaHeaders(Map()),
      cannedAcl: CannedAcl = CannedAcl.Private,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      maxRetriesPerChunk: Int = 3,
      sse: Option[ServerSideEncryption] = None
  ): Sink[ByteString, Future[MultipartUploadResult]] = {
    val headers =
      S3Headers.empty.withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    multipartUploadWithHeaders(bucket, key, contentType, chunkSize, chunkingParallelism, maxRetriesPerChunk, headers)
  }

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param maxRetriesPerChunk the maximum number of times a given chunk upload will be retried on transient errors
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUploadWithHeaders(
      bucket: String,
      key: String,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      maxRetriesPerChunk: Int = 3,
      s3Headers: S3Headers = S3Headers.empty
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    S3Stream
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism,
        maxRetriesPerChunk
      )

  /**
   * Copy an S3 object from source bucket to target bucket using multi part copy upload.
   *
   * @param sourceBucket source s3 bucket name
   * @param sourceKey    source s3 key
   * @param targetBucket target s3 bucket name
   * @param targetKey    target s3 key
   * @param sourceVersionId optional version id of source object, if the versioning is enabled in source bucket
   * @param contentType  an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a runnable graph which upon materialization will return a [[scala.concurrent.Future Future ]] containing the results of the copy operation.
   */
  def multipartCopy(
      sourceBucket: String,
      sourceKey: String,
      targetBucket: String,
      targetKey: String,
      sourceVersionId: Option[String] = None,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: S3Headers = S3Headers.empty,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): RunnableGraph[Future[MultipartUploadResult]] =
    S3Stream
      .multipartCopy(
        S3Location(sourceBucket, sourceKey),
        S3Location(targetBucket, targetKey),
        sourceVersionId,
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] with type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes = Attributes()): Future[Done] =
    makeBucket(bucketName, S3Headers.empty)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[scala.concurrent.Future Future]] with type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String, s3Headers: S3Headers)(implicit mat: Materializer, attr: Attributes): Future[Done] =
    S3Stream.makeBucket(bucketName, s3Headers)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucketSource(bucketName: String): Source[Done, NotUsed] =
    makeBucketSource(bucketName, S3Headers.empty)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucketSource(bucketName: String, s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.makeBucketSource(bucketName, s3Headers)

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String)(implicit mat: Materializer,
                                       attributes: Attributes = Attributes()): Future[Done] =
    deleteBucket(bucketName, S3Headers.empty)

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[scala.concurrent.Future Future]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(
      bucketName: String,
      s3Headers: S3Headers
  )(implicit mat: Materializer, attributes: Attributes): Future[Done] =
    S3Stream.deleteBucket(bucketName, s3Headers)

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    deleteBucketSource(bucketName, S3Headers.empty)

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucketSource(bucketName: String, s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteBucketSource(bucketName, s3Headers)

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String)(implicit mat: Materializer,
                                              attributes: Attributes = Attributes()): Future[BucketAccess] =
    checkIfBucketExists(bucketName, S3Headers.empty)

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[scala.concurrent.Future Future]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(
      bucketName: String,
      s3Headers: S3Headers
  )(implicit mat: Materializer, attributes: Attributes): Future[BucketAccess] =
    S3Stream.checkIfBucketExists(bucketName, s3Headers)

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[BucketAccess]]
   */
  def checkIfBucketExistsSource(bucketName: String): Source[BucketAccess, NotUsed] =
    checkIfBucketExistsSource(bucketName, S3Headers.empty)

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of type [[BucketAccess]]
   */
  def checkIfBucketExistsSource(bucketName: String, s3Headers: S3Headers): Source[BucketAccess, NotUsed] =
    S3Stream.checkIfBucketExistsSource(bucketName, s3Headers)
}
