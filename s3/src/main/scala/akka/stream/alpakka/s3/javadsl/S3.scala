/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.japi.{Pair => JPair}
import akka.{Done, NotUsed}
import akka.http.javadsl.model._
import akka.http.javadsl.model.headers.ByteRange
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.s3.headers.ServerSideEncryption
import akka.stream.alpakka.s3._
import akka.stream.javadsl.{RunnableGraph, Sink, Source}
import akka.util.ByteString

/**
 * Java API
 *
 * Factory of S3 operations.
 */
object S3 {

  /**
   * Use this for a low level access to S3.
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a raw HTTP response from S3
   */
  def request(bucket: String, key: String, method: HttpMethod, s3Headers: S3Headers): Source[HttpResponse, NotUsed] =
    S3WithHeaders.request(bucket, key, method, s3Headers)

  /**
   * Use this for a low level access to S3.
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a raw HTTP response from S3
   */
  def request(bucket: String,
              key: String,
              versionId: Optional[String],
              method: HttpMethod = HttpMethods.GET,
              s3Headers: S3Headers = S3Headers()): Source[HttpResponse, NotUsed] =
    S3WithHeaders.request(bucket, key, versionId, method, s3Headers)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String): Source[Optional[ObjectMetadata], NotUsed] =
    S3WithHeaders.getObjectMetadata(bucket, key)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: ServerSideEncryption): Source[Optional[ObjectMetadata], NotUsed] =
    S3WithHeaders.getObjectMetadata(bucket, key, S3Headers().withOptionalServerSideEncryption(Option(sse)))

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param sse the server side encryption to use
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Optional[String],
                        sse: ServerSideEncryption): Source[Optional[ObjectMetadata], NotUsed] =
    S3WithHeaders.getObjectMetadata(bucket, key, versionId, S3Headers().withOptionalServerSideEncryption(Option(sse)))

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObject(bucket: String, key: String): Source[Done, NotUsed] =
    S3WithHeaders.deleteObject(bucket, key)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObject(bucket: String, key: String, versionId: Optional[String]): Source[Done, NotUsed] =
    S3WithHeaders.deleteObject(bucket, key, versionId, S3Headers())

  /**
   * Deletes all keys under the specified bucket
   *
   * @param bucket the s3 bucket name
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String): Source[Done, NotUsed] = S3WithHeaders.deleteObjectsByPrefix(bucket)

  /**
   * Deletes all keys which have the given prefix under the specified bucket
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Optional[String]): Source[Done, NotUsed] =
    S3WithHeaders.deleteObjectsByPrefix(bucket, prefix, S3Headers())

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any additional headers for the request
   * @return a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                s3Headers: S3Headers): Source[ObjectMetadata, NotUsed] =
    S3WithHeaders.putObject(bucket, key, data, contentLength, contentType, s3Headers)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType): Source[ObjectMetadata, NotUsed] =
    S3WithHeaders.putObject(bucket, key, data, contentLength, contentType)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @return a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long): Source[ObjectMetadata, NotUsed] =
    S3WithHeaders.putObject(bucket, key, data, contentLength)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    S3WithHeaders.download(bucket, key)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      sse: ServerSideEncryption
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    S3WithHeaders.download(bucket, key, S3Headers().withOptionalServerSideEncryption(Option(sse)))

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    S3WithHeaders.download(bucket, key, range)

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      sse: ServerSideEncryption
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    S3WithHeaders.download(bucket, key, range, S3Headers().withOptionalServerSideEncryption(Option(sse)))

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      versionId: Optional[String],
      sse: ServerSideEncryption
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    S3WithHeaders.download(bucket, key, range, versionId, S3Headers().withOptionalServerSideEncryption(Option(sse)))

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The <code>akka.stream.alpakka.s3.list-bucket-api-version</code> can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String]): Source[ListBucketResultContents, NotUsed] =
    S3WithHeaders.listBucket(bucket, prefix, S3Headers())

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartUpload(bucket, key, contentType, s3Headers)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartUpload(bucket, key, contentType)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartUpload(bucket, key)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param sourceVersionId version id of source object, if the versioning is enabled in source bucket
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @return the [[akka.stream.alpakka.s3.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    contentType: ContentType,
                    s3Headers: S3Headers): RunnableGraph[CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket,
                                sourceKey,
                                targetBucket,
                                targetKey,
                                sourceVersionId,
                                contentType,
                                s3Headers)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param sourceVersionId version id of source object, if the versioning is enabled in source bucket
   * @param s3Headers any headers you want to add
   * @return the [[akka.stream.alpakka.s3.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    s3Headers: S3Headers): RunnableGraph[CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, sourceVersionId, s3Headers)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @return the [[akka.stream.alpakka.s3.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    contentType: ContentType,
                    s3Headers: S3Headers): RunnableGraph[CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, contentType, s3Headers)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param s3Headers any headers you want to add
   * @return the [[akka.stream.alpakka.s3.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    s3Headers: S3Headers): RunnableGraph[CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, s3Headers)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @return the [[akka.stream.alpakka.s3.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String): RunnableGraph[CompletionStage[MultipartUploadResult]] =
    S3WithHeaders.multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String, materializer: Materializer, attributes: Attributes): CompletionStage[Done] =
    S3WithHeaders.makeBucket(bucketName, materializer, attributes, S3Headers())

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String, materializer: Materializer): CompletionStage[Done] =
    S3WithHeaders.makeBucket(bucketName, materializer)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @return [[akka.stream.javadsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucketSource(bucketName: String): Source[Done, NotUsed] =
    S3WithHeaders.makeBucketSource(bucketName, S3Headers())

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String, materializer: Materializer, attributes: Attributes): CompletionStage[Done] =
    S3WithHeaders.deleteBucket(bucketName, materializer, attributes, S3Headers())

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String, materializer: Materializer): CompletionStage[Done] =
    S3WithHeaders.deleteBucket(bucketName, materializer)

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @return [[akka.stream.javadsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    S3WithHeaders.deleteBucketSource(bucketName, S3Headers())

  /**
   * Checks whether the bucket exists and the user has rights to perform the `ListBucket` operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String,
                          materializer: Materializer,
                          attributes: Attributes): CompletionStage[BucketAccess] =
    S3WithHeaders.checkIfBucketExists(bucketName, materializer, attributes, S3Headers())

  /**
   * Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String, materializer: Materializer): CompletionStage[BucketAccess] =
    S3WithHeaders.checkIfBucketExists(bucketName, materializer)

  /**
   * Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @return [[akka.stream.javadsl.Source Source]] of type [[BucketAccess]]
   */
  def checkIfBucketExistsSource(bucketName: String): Source[BucketAccess, NotUsed] =
    S3WithHeaders.checkIfBucketExistsSource(bucketName, S3Headers())
}
