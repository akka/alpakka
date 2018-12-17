/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl
import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.S3Ext
import akka.stream.alpakka.s3.S3Client.MinChunkSize
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.impl.{MetaHeaders, S3Headers, ServerSideEncryption}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

/**
 * Factory of S3 operations that use provided model.scala.
 */
object S3 {

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[akka.http.scaladsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return a [[scala.concurrent.Future Future]] containing the raw [[akka.http.scaladsl.model.HttpResponse HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod = HttpMethods.GET,
              versionId: Option[String] = None,
              s3Headers: S3Headers = S3Headers.empty)(implicit sys: ActorSystem): Future[HttpResponse] =
    S3External.request(bucket, key, method, versionId, s3Headers)(S3Ext(sys).client)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[scala.concurrent.Future Future]] containing an [[scala.Option]] that will be [[scala.None]] in case the object does not exist
   */
  def getObjectMetadata(
      bucket: String,
      key: String,
      versionId: Option[String] = None,
      sse: Option[ServerSideEncryption] = None
  )(implicit sys: ActorSystem): Future[Option[ObjectMetadata]] =
    S3External.getObjectMetadata(bucket, key, versionId, sse)(S3Ext(sys).client)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version idof the object
   * @return A [[scala.concurrent.Future Future]] of [[akka.Done]]
   */
  def deleteObject(bucket: String, key: String, versionId: Option[String] = None)(
      implicit sys: ActorSystem
  ): Future[Done] =
    S3External.deleteObject(bucket, key, versionId)(S3Ext(sys).client)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[Stream]] of [[ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[scala.concurrent.Future Future]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType = ContentTypes.`application/octet-stream`,
                s3Headers: S3Headers,
                sse: Option[ServerSideEncryption] = None)(implicit sys: ActorSystem): Future[ObjectMetadata] =
    S3External.putObject(bucket, key, data, contentLength, contentType, s3Headers, sse)(S3Ext(sys).client)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range [optional] the [[akka.http.scaladsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse [optional] the server side encryption used on upload
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] and a [[scala.concurrent.Future Future]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: Option[ByteRange] = None,
      versionId: Option[String] = None,
      sse: Option[ServerSideEncryption] = None
  )(implicit sys: ActorSystem): Future[Option[(Source[ByteString, NotUsed], ObjectMetadata)]] =
    S3External.download(bucket, key, range, versionId, sse)(S3Ext(sys).client)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The `akka.stream.alpakka.s3.list-bucket-api-version` can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   *
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return [[akka.stream.scaladsl.Source Source]] of [[ListBucketResultContents]]
   */
  def listBucket(bucket: String,
                 prefix: Option[String])(implicit sys: ActorSystem): Source[ListBucketResultContents, NotUsed] =
    S3External.listBucket(bucket, prefix)(S3Ext(sys).client)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.scaladsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defauts to [[CannedAcl.Private]]
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
  )(implicit sys: ActorSystem): Sink[ByteString, Future[MultipartUploadResult]] =
    S3External.multipartUpload(bucket, key, contentType, metaHeaders, cannedAcl, chunkSize, chunkingParallelism, sse)(
      S3Ext(sys).client
    )

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
      s3Headers: Option[S3Headers] = None,
      sse: Option[ServerSideEncryption] = None
  )(implicit sys: ActorSystem): Sink[ByteString, Future[MultipartUploadResult]] =
    S3External.multipartUploadWithHeaders(bucket, key, contentType, chunkSize, chunkingParallelism, s3Headers, sse)(
      S3Ext(sys).client
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
   * @param sse an optional server side encryption key
   * @param chunkSize the size of the requests sent to S3, minimum [[MinChunkSize]]
   * @param chunkingParallelism the number of parallel requests used for the upload, defaults to 4
   * @return
   */
  def multipartCopy(
      sourceBucket: String,
      sourceKey: String,
      targetBucket: String,
      targetKey: String,
      sourceVersionId: Option[String] = None,
      contentType: ContentType = ContentTypes.`application/octet-stream`,
      s3Headers: Option[S3Headers] = None,
      sse: Option[ServerSideEncryption] = None,
      chunkSize: Int = MinChunkSize,
      chunkingParallelism: Int = 4
  )(implicit sys: ActorSystem): Future[MultipartUploadResult] =
    S3External.multipartCopy(sourceBucket,
                             sourceKey,
                             targetBucket,
                             targetKey,
                             sourceVersionId,
                             contentType,
                             s3Headers,
                             sse,
                             chunkSize,
                             chunkingParallelism)(S3Ext(sys).client)
}
