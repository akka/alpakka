/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl
import akka.{Done, NotUsed}
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model._
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption}
import akka.stream.alpakka.s3._
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
              s3Headers: S3Headers = S3Headers()): Source[HttpResponse, NotUsed] =
    S3WithHeaders.request(bucket, key, method, versionId, s3Headers)

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
    S3WithHeaders.getObjectMetadata(bucket, key, versionId, S3Headers().withOptionalServerSideEncryption(sse))

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObject(bucket: String, key: String, versionId: Option[String] = None): Source[Done, NotUsed] =
    S3WithHeaders.deleteObject(bucket, key, versionId, S3Headers())

  /**
   * Deletes a S3 Objects which contain given prefix
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String]): Source[Done, NotUsed] =
    S3WithHeaders.deleteObjectsByPrefix(bucket, prefix, S3Headers())

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
    S3WithHeaders.putObject(bucket, key, data, contentLength, contentType, s3Headers)

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
    S3WithHeaders.download(bucket, key, range, versionId, S3Headers().withOptionalServerSideEncryption(sse))

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    S3WithHeaders.listBucket(bucket, prefix, S3Headers())

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
      sse: Option[ServerSideEncryption] = None
  ): Sink[ByteString, Future[MultipartUploadResult]] = {
    val headers =
      S3Headers().withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    S3WithHeaders.multipartUpload(bucket, key, contentType, chunkSize, chunkingParallelism, headers)
  }

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUploadWithHeaders(
      bucket: String,
      key: String,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: S3Headers = S3Headers()
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    S3WithHeaders.multipartUpload(bucket, key, contentType, chunkSize, chunkingParallelism, s3Headers)

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
      s3Headers: S3Headers = S3Headers(),
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  ): RunnableGraph[Future[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket,
                                sourceKey,
                                targetBucket,
                                targetKey,
                                sourceVersionId,
                                contentType,
                                s3Headers,
                                chunkSize,
                                chunkingParallelism)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] with type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes = Attributes()): Future[Done] =
    S3WithHeaders.makeBucket(bucketName, S3Headers())

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucketSource(bucketName: String): Source[Done, NotUsed] =
    S3WithHeaders.makeBucketSource(bucketName, S3Headers())

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String)(implicit mat: Materializer,
                                       attributes: Attributes = Attributes()): Future[Done] =
    S3WithHeaders.deleteBucket(bucketName, S3Headers())

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    S3WithHeaders.deleteBucketSource(bucketName, S3Headers())

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   *
   * @param bucketName bucket name
   * @return [[scala.concurrent.Future Future]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String)(implicit mat: Materializer,
                                              attributes: Attributes = Attributes()): Future[BucketAccess] =
    S3WithHeaders.checkIfBucketExists(bucketName, S3Headers())

  /**
   *   Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   *
   * @param bucketName bucket name
   * @return [[akka.stream.scaladsl.Source Source]] of type [[BucketAccess]]
   */
  def checkIfBucketExistsSource(bucketName: String): Source[BucketAccess, NotUsed] =
    S3WithHeaders.checkIfBucketExistsSource(bucketName, S3Headers())
}
