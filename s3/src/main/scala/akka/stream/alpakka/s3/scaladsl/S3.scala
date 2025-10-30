/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.s3.scaladsl
import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Attributes
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption}
import akka.stream.alpakka.s3.impl._
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.collection.immutable
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
   * @param deleteAllVersions Whether to delete all object versions as well (applies to versioned buckets)
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String], deleteAllVersions: Boolean): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, prefix, deleteAllVersions, S3Headers.empty)

  /**
   * Deletes a S3 Objects which contain given prefix
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Option[String], s3Headers: S3Headers): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, prefix, deleteAllVersions = false, s3Headers)

  /**
   * Deletes a S3 Objects which contain given prefix
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @param deleteAllVersions Whether to delete all object versions as well (applies to versioned buckets)
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String,
                            prefix: Option[String],
                            deleteAllVersions: Boolean,
                            s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteObjectsByPrefix(bucket, prefix, deleteAllVersions, s3Headers)

  /**
   * Deletes all S3 Objects within the given bucket
   *
   * @param bucket the s3 bucket name
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteBucketContents(bucket: String): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, None, S3Headers.empty)

  /**
   * Deletes all S3 Objects within the given bucket
   *
   * @param bucket the s3 bucket name
   * @param deleteAllVersions Whether to delete all object versions as well (applies to versioned buckets)
   * @return A [[akka.stream.scaladsl.Source Source]] that will emit [[akka.Done]] when operation is completed
   */
  def deleteBucketContents(bucket: String, deleteAllVersions: Boolean): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, None, deleteAllVersions, S3Headers.empty)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]]
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
  @deprecated("Use S3.getObject instead", "4.0.0")
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
  @deprecated("Use S3.getObject instead", "4.0.0")
  def download(
      bucket: String,
      key: String,
      range: Option[ByteRange],
      versionId: Option[String],
      s3Headers: S3Headers
  ): Source[Option[(Source[ByteString, NotUsed], ObjectMetadata)], NotUsed] =
    S3Stream.download(S3Location(bucket, key), range, versionId, s3Headers)

  /**
   * Gets a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse [optional] the server side encryption used on upload
   * @return A [[akka.stream.scaladsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a materialized value containing the
   *         [[akka.stream.alpakka.s3.ObjectMetadata]]
   */
  def getObject(
      bucket: String,
      key: String,
      range: Option[ByteRange] = None,
      versionId: Option[String] = None,
      sse: Option[ServerSideEncryption] = None
  ): Source[ByteString, Future[ObjectMetadata]] =
    getObject(bucket, key, range, versionId, S3Headers.empty.withOptionalServerSideEncryption(sse))

  /**
   * Gets a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.scaladsl.Source]] containing the objects data as a [[akka.util.ByteString]] along with a materialized value containing the
   *         [[akka.stream.alpakka.s3.ObjectMetadata]]
   */
  def getObject(
      bucket: String,
      key: String,
      range: Option[ByteRange],
      versionId: Option[String],
      s3Headers: S3Headers
  ): Source[ByteString, Future[ObjectMetadata]] =
    S3Stream.getObject(S3Location(bucket, key), range, versionId, s3Headers)

  /**
   * Will return a list containing all of the buckets for the current AWS account
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketsResultContents]]
   */
  def listBuckets(): Source[ListBucketsResultContents, NotUsed] =
    listBuckets(S3Headers.empty)

  /**
   * Will return a list containing all of the buckets for the current AWS account
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketsResultContents]]
   */
  def listBuckets(s3Headers: S3Headers): Source[ListBucketsResultContents, NotUsed] =
    S3Stream.listBuckets(s3Headers)

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
   * Will return in progress or aborted multipart uploads. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
   * @param bucket Which bucket that you list in-progress multipart uploads for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListMultipartUploadResultUploads]]
   */
  def listMultipartUpload(bucket: String, prefix: Option[String]): Source[ListMultipartUploadResultUploads, NotUsed] =
    listMultipartUpload(bucket, prefix, S3Headers.empty)

  /**
   * Will return in progress or aborted multipart uploads. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
   * @param bucket Which bucket that you list in-progress multipart uploads for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListMultipartUploadResultUploads]]
   */
  def listMultipartUpload(bucket: String,
                          prefix: Option[String],
                          s3Headers: S3Headers): Source[ListMultipartUploadResultUploads, NotUsed] =
    S3Stream.listMultipartUpload(bucket, prefix, s3Headers)

  /**
   * Will return in progress or aborted multipart uploads with optional prefix and delimiter. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
   * @param bucket Which bucket that you list in-progress multipart uploads for
   * @param delimiter Delimiter to use for listing only one level of hierarchy
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListMultipartUploadResultUploads ListMultipartUploadResultUploads]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.CommonPrefixes CommonPrefixes]])
   */
  def listMultipartUploadAndCommonPrefixes(
      bucket: String,
      delimiter: String,
      prefix: Option[String] = None,
      s3Headers: S3Headers = S3Headers.empty
  ): Source[(Seq[ListMultipartUploadResultUploads], Seq[CommonPrefixes]), NotUsed] =
    S3Stream.listMultipartUploadAndCommonPrefixes(bucket, delimiter, prefix, s3Headers)

  /**
   * List uploaded parts for a specific upload. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
   * @param bucket Under which bucket the upload parts are contained
   * @param key They key where the parts were uploaded to
   * @param uploadId Unique identifier of the upload for which you want to list the uploaded parts
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListPartsResultParts]]
   */
  def listParts(bucket: String, key: String, uploadId: String): Source[ListPartsResultParts, NotUsed] =
    listParts(bucket, key, uploadId, S3Headers.empty)

  /**
   * List uploaded parts for a specific upload. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
   * @param bucket Under which bucket the upload parts are contained
   * @param key They key where the parts were uploaded to
   * @param uploadId Unique identifier of the upload for which you want to list the uploaded parts
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListPartsResultParts]]
   */
  def listParts(bucket: String,
                key: String,
                uploadId: String,
                s3Headers: S3Headers): Source[ListPartsResultParts, NotUsed] =
    S3Stream.listParts(bucket, key, uploadId, s3Headers)

  /**
   * List all versioned objects for a bucket with optional prefix. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
   * @param bucket Which bucket that you list object versions for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListObjectVersionsResultVersions ListObjectVersionsResultVersions]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.DeleteMarkers DeleteMarkers]])
   */
  def listObjectVersions(
      bucket: String,
      prefix: Option[String]
  ): Source[(Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers]), NotUsed] =
    S3Stream.listObjectVersions(bucket, prefix, S3Headers.empty)

  /**
   * List all versioned objects for a bucket with optional prefix. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
   * @param bucket Which bucket that you list object versions for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListObjectVersionsResultVersions ListObjectVersionsResultVersions]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.DeleteMarkers DeleteMarkers]])
   */
  def listObjectVersions(
      bucket: String,
      prefix: Option[String],
      s3Headers: S3Headers
  ): Source[(Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers]), NotUsed] =
    S3Stream.listObjectVersions(bucket, prefix, s3Headers)

  /**
   * List all versioned objects for a bucket with optional prefix and delimiter. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
   * @param bucket Which bucket that you list object versions for
   * @param delimiter Delimiter to use for listing only one level of hierarchy
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListObjectVersionsResultVersions ListObjectVersionsResultVersions]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.DeleteMarkers DeleteMarkers]])
   */
  def listObjectVersions(
      bucket: String,
      delimiter: String,
      prefix: Option[String],
      s3Headers: S3Headers
  ): Source[(Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers]), NotUsed] =
    S3Stream.listObjectVersionsAndCommonPrefixes(bucket, delimiter, prefix, s3Headers).map {
      case (versions, markers, _) =>
        (versions, markers)
    }

  /**
   * List all versioned objects for a bucket with optional prefix and delimiter. This will automatically page through all keys with the given parameters.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
   * @param bucket Which bucket that you list object versions for
   * @param delimiter Delimiter to use for listing only one level of hierarchy
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of ([[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.ListObjectVersionsResultVersions ListObjectVersionsResultVersions]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.DeleteMarkers DeleteMarkers]], [[scala.collection.Seq Seq]] of [[akka.stream.alpakka.s3.CommonPrefixes CommonPrefixes]])
   */
  def listObjectVersionsAndCommonPrefixes(bucket: String,
                                          delimiter: String,
                                          prefix: Option[String],
                                          s3Headers: S3Headers): Source[
    (Seq[ListObjectVersionsResultVersions], Seq[DeleteMarkers], Seq[CommonPrefixes]),
    NotUsed
  ] =
    S3Stream.listObjectVersionsAndCommonPrefixes(bucket, delimiter, prefix, s3Headers)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl]], defaults to [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
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
      S3Headers.empty.withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    multipartUploadWithHeaders(bucket, key, contentType, chunkSize, chunkingParallelism, headers)
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
      s3Headers: S3Headers = S3Headers.empty
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    S3Stream
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )

  /**
   * Uploads a S3 Object by making multiple requests. Unlike `multipartUpload`, this version allows you to pass in a
   * context (typically from a `SourceWithContext`/`FlowWithContext`) along with a chunkUploadSink that defines how to
   * act whenever a chunk is uploaded.
   *
   * Note that this version of resuming multipart upload ignores buffering
   *
   * @tparam C The Context that is passed along with the `ByteString`
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param chunkUploadSink A sink that's a callback which gets executed whenever an entire Chunk is uploaded to S3
   *                        (successfully or unsuccessfully). Since each chunk can contain more than one emitted element
   *                        from the original flow/source you get provided with the list of context's.
   *
   *                        The internal implementation uses `Flow.alsoTo` for `chunkUploadSink` which means that
   *                        backpressure is applied to the upload stream if `chunkUploadSink` is too slow, likewise any
   *                        failure will also be propagated to the upload stream. Sink Materialization is also shared
   *                        with the returned `Sink`.
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl]], defaults to [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts ([[akka.util.ByteString ByteString]], `C`)'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUploadWithContext[C](
      bucket: String,
      key: String,
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      metaHeaders: MetaHeaders = MetaHeaders(Map()),
      cannedAcl: CannedAcl = CannedAcl.Private,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      sse: Option[ServerSideEncryption] = None
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] = {
    val headers =
      S3Headers.empty.withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    multipartUploadWithHeadersAndContext(bucket,
                                         key,
                                         chunkUploadSink,
                                         contentType,
                                         chunkSize,
                                         chunkingParallelism,
                                         headers)
  }

  /**
   * Uploads a S3 Object by making multiple requests. Unlike `multipartUploadWithHeaders`, this version allows you to
   * pass in a context (typically from a `SourceWithContext`/`FlowWithContext`) along with a chunkUploadSink that
   * defines how to act whenever a chunk is uploaded.
   *
   * Note that this version of resuming multipart upload ignores buffering
   *
   * @tparam C The Context that is passed along with the `ByteString`
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param chunkUploadSink A sink that's a callback which gets executed whenever an entire Chunk is uploaded to S3
   *                        (successfully or unsuccessfully). Since each chunk can contain more than one emitted element
   *                        from the original flow/source you get provided with the list of context's.
   *
   *                        The internal implementation uses `Flow.alsoTo` for `chunkUploadSink` which means that
   *                        backpressure is applied to the upload stream if `chunkUploadSink` is too slow, likewise any
   *                        failure will also be propagated to the upload stream. Sink Materialization is also shared
   *                        with the returned `Sink`.
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts ([[akka.util.ByteString ByteString]], `C`)'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def multipartUploadWithHeadersAndContext[C](
      bucket: String,
      key: String,
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: S3Headers = S3Headers.empty
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] =
    S3Stream
      .multipartUploadWithContext(
        S3Location(bucket, key),
        chunkUploadSink,
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )

  /**
   * Resumes from a previously aborted multipart upload by providing the uploadId and previous upload part identifiers
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to resume
   * @param previousParts The previously uploaded parts ending just before when this upload will commence
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl]], defaults to [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def resumeMultipartUpload(
      bucket: String,
      key: String,
      uploadId: String,
      previousParts: immutable.Iterable[Part],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      metaHeaders: MetaHeaders = MetaHeaders(Map()),
      cannedAcl: CannedAcl = CannedAcl.Private,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      sse: Option[ServerSideEncryption] = None
  ): Sink[ByteString, Future[MultipartUploadResult]] = {
    val headers =
      S3Headers.empty.withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    resumeMultipartUploadWithHeaders(bucket,
                                     key,
                                     uploadId,
                                     previousParts,
                                     contentType,
                                     chunkSize,
                                     chunkingParallelism,
                                     headers)
  }

  /**
   * Resumes from a previously aborted multipart upload by providing the uploadId and previous upload part identifiers.
   * Unlike `resumeMultipartUpload`, this version allows you to pass in a context (typically from a
   * `SourceWithContext`/`FlowWithContext`) along with a chunkUploadSink that defines how to act whenever a chunk is
   * uploaded.
   *
   * Note that this version of resuming multipart upload ignores buffering
   *
   * @tparam C The Context that is passed along with the `ByteString`
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to resume
   * @param previousParts The previously uploaded parts ending just before when this upload will commence
   * @param chunkUploadSink A sink that's a callback which gets executed whenever an entire Chunk is uploaded to S3
   *                        (successfully or unsuccessfully). Since each chunk can contain more than one emitted element
   *                        from the original flow/source you get provided with the list of context's.
   *
   *                        The internal implementation uses `Flow.alsoTo` for `chunkUploadSink` which means that
   *                        backpressure is applied to the upload stream if `chunkUploadSink` is too slow, likewise any
   *                        failure will also be propagated to the upload stream. Sink Materialization is also shared
   *                        with the returned `Sink`.
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl]], defaults to [[akka.stream.alpakka.s3.headers.CannedAcl CannedAcl.Private]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts ([[akka.util.ByteString ByteString]], `C`)'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def resumeMultipartUploadWithContext[C](
      bucket: String,
      key: String,
      uploadId: String,
      previousParts: immutable.Iterable[Part],
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      metaHeaders: MetaHeaders = MetaHeaders(Map()),
      cannedAcl: CannedAcl = CannedAcl.Private,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      sse: Option[ServerSideEncryption] = None
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] = {
    val headers =
      S3Headers.empty.withCannedAcl(cannedAcl).withMetaHeaders(metaHeaders).withOptionalServerSideEncryption(sse)
    resumeMultipartUploadWithHeadersAndContext(bucket,
                                               key,
                                               uploadId,
                                               previousParts,
                                               chunkUploadSink,
                                               contentType,
                                               chunkSize,
                                               chunkingParallelism,
                                               headers)
  }

  /**
   * Resumes from a previously aborted multipart upload by providing the uploadId and previous upload part identifiers.
   * Unlike `resumeMultipartUpload`, this version allows you to pass in a context (typically from a
   * `SourceWithContext`/`FlowWithContext`) along with a chunkUploadSink that defines how to act whenever a chunk is
   * uploaded.
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to resume
   * @param previousParts The previously uploaded parts ending just before when this upload will commence
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def resumeMultipartUploadWithHeaders(
      bucket: String,
      key: String,
      uploadId: String,
      previousParts: immutable.Iterable[Part],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: S3Headers = S3Headers.empty
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    S3Stream
      .resumeMultipartUpload(
        S3Location(bucket, key),
        uploadId,
        previousParts,
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )

  /**
   * Resumes from a previously aborted multipart upload by providing the uploadId and previous upload part identifiers.
   * Unlike `resumeMultipartUploadWithHeaders`, this version allows you to pass in a context (typically from a
   * `SourceWithContext`/`FlowWithContext`) along with a chunkUploadSink that defines how to act whenever a chunk is
   * uploaded.
   *
   * Note that this version of resuming multipart upload ignores buffering
   *
   * @tparam C The Context that is passed along with the `ByteString`
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to resume
   * @param previousParts The previously uploaded parts ending just before when this upload will commence
   * @param chunkUploadSink A sink that's a callback which gets executed whenever an entire Chunk is uploaded to S3
   *                        (successfully or unsuccessfully). Since each chunk can contain more than one emitted element
   *                        from the original flow/source you get provided with the list of context's.
   *
   *                        The internal implementation uses `Flow.alsoTo` for `chunkUploadSink` which means that
   *                        backpressure is applied to the upload stream if `chunkUploadSink` is too slow, likewise any
   *                        failure will also be propagated to the upload stream. Sink Materialization is also shared
   *                        with the returned `Sink`.
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @param s3Headers any headers you want to add
   * @return a [[akka.stream.scaladsl.Sink Sink]] that accepts ([[akka.util.ByteString ByteString]], `C`)'s and materializes to a [[scala.concurrent.Future Future]] of [[MultipartUploadResult]]
   */
  def resumeMultipartUploadWithHeadersAndContext[C](
      bucket: String,
      key: String,
      uploadId: String,
      previousParts: immutable.Iterable[Part],
      chunkUploadSink: Sink[(UploadPartResponse, immutable.Iterable[C]), NotUsed],
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4,
      s3Headers: S3Headers = S3Headers.empty
  ): Sink[(ByteString, C), Future[MultipartUploadResult]] =
    S3Stream
      .resumeMultipartUploadWithContext(
        S3Location(bucket, key),
        uploadId,
        previousParts,
        chunkUploadSink,
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )

  /**
   * Complete a multipart upload with an already given list of parts
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to complete
   * @param parts A list of all of the parts for the multipart upload
   *
   * @return [[scala.concurrent.Future Future]] of type [[MultipartUploadResult]]
   */
  def completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: immutable.Iterable[Part])(
      implicit system: ClassicActorSystemProvider,
      attributes: Attributes = Attributes()
  ): Future[MultipartUploadResult] =
    S3Stream.completeMultipartUpload(S3Location(bucket, key), uploadId, parts, S3Headers.empty)

  /**
   * Complete a multipart upload with an already given list of parts
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param uploadId the upload that you want to complete
   * @param parts A list of all of the parts for the multipart upload
   * @param s3Headers any headers you want to add
   *
   * @return [[scala.concurrent.Future Future]] of type [[MultipartUploadResult]]
   */
  def completeMultipartUpload(
      bucket: String,
      key: String,
      uploadId: String,
      parts: immutable.Iterable[Part],
      s3Headers: S3Headers
  )(implicit system: ClassicActorSystemProvider, attributes: Attributes): Future[MultipartUploadResult] =
    S3Stream.completeMultipartUpload(S3Location(bucket, key), uploadId, parts, s3Headers)

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
  def makeBucket(bucketName: String)(implicit system: ClassicActorSystemProvider,
                                     attr: Attributes = Attributes()): Future[Done] =
    S3Stream.makeBucket(bucketName, S3Headers.empty)

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[scala.concurrent.Future Future]] with type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String, s3Headers: S3Headers)(implicit system: ClassicActorSystemProvider,
                                                           attr: Attributes): Future[Done] =
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
  def deleteBucket(bucketName: String)(implicit system: ClassicActorSystemProvider,
                                       attributes: Attributes = Attributes()): Future[Done] =
    S3Stream.deleteBucket(bucketName, S3Headers.empty)

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
  )(implicit system: ClassicActorSystemProvider, attributes: Attributes): Future[Done] =
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
  def checkIfBucketExists(bucketName: String)(implicit system: ClassicActorSystemProvider,
                                              attributes: Attributes = Attributes()): Future[BucketAccess] =
    S3Stream.checkIfBucketExists(bucketName, S3Headers.empty)

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
  )(implicit system: ClassicActorSystemProvider, attributes: Attributes): Future[BucketAccess] =
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

  /**
   * Delete all existing parts for a specific upload id
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
   *
   * @param bucketName Which bucket the upload is inside
   * @param key The key for the upload
   * @param uploadId Unique identifier of the upload
   * @return [[scala.concurrent.Future Future]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteUpload(bucketName: String, key: String, uploadId: String)(
      implicit system: ClassicActorSystemProvider,
      attributes: Attributes = Attributes()
  ): Future[Done] =
    deleteUpload(bucketName, key, uploadId, S3Headers.empty)

  /**
   * Delete all existing parts for a specific upload
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
   * @param bucketName Which bucket the upload is inside
   * @param key The key for the upload
   * @param uploadId Unique identifier of the upload
   * @param s3Headers any headers you want to add
   * @return [[scala.concurrent.Future Future]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteUpload(
      bucketName: String,
      key: String,
      uploadId: String,
      s3Headers: S3Headers
  )(implicit system: ClassicActorSystemProvider, attributes: Attributes): Future[Done] =
    S3Stream.deleteUpload(bucketName, key, uploadId, s3Headers)

  /**
   * Delete all existing parts for a specific upload
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
   * @param bucketName Which bucket the upload is inside
   * @param key The key for the upload
   * @param uploadId Unique identifier of the upload
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteUploadSource(bucketName: String, key: String, uploadId: String): Source[Done, NotUsed] =
    deleteUploadSource(bucketName, key, uploadId, S3Headers.empty)

  /**
   * Delete all existing parts for a specific upload
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
   *
   * @param bucketName Which bucket the upload is inside
   * @param key The key for the upload
   * @param uploadId Unique identifier of the upload
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.scaladsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteUploadSource(bucketName: String,
                         key: String,
                         uploadId: String,
                         s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteUploadSource(bucketName, key, uploadId, s3Headers)
}
