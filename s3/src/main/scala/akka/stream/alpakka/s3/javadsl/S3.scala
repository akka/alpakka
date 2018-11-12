/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import akka.japi.{Pair => JPair}
import akka.{Done, NotUsed}
import akka.http.javadsl.model._
import akka.http.javadsl.model.headers.ByteRange
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.impl.{MetaHeaders, S3Headers, ServerSideEncryption}
import akka.stream.alpakka.s3.S3ClientExt
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString

object S3 {

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              method: HttpMethod,
              s3Headers: S3Headers,
              sys: ActorSystem): CompletionStage[HttpResponse] =
    request(bucket, key, Optional.empty(), method, s3Headers, sys)

  /**
   * Use this to extend the library
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param method the [[akka.http.javadsl.model.HttpMethod HttpMethod]] to use when making the request
   * @param s3Headers any headers you want to add
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the raw [[HttpResponse]]
   */
  def request(bucket: String,
              key: String,
              versionId: Optional[String],
              method: HttpMethod = HttpMethods.GET,
              s3Headers: S3Headers = S3Headers.empty,
              sys: ActorSystem): CompletionStage[HttpResponse] =
    S3External.request(bucket, key, versionId, method, s3Headers, S3ClientExt.get(sys).client)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String, sys: ActorSystem): CompletionStage[Optional[ObjectMetadata]] =
    getObjectMetadata(bucket, key, null, sys)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        sse: ServerSideEncryption,
                        sys: ActorSystem): CompletionStage[Optional[ObjectMetadata]] =
    getObjectMetadata(bucket, key, Optional.empty(), sse, sys)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param sse the server side encryption to use
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Optional[String],
                        sse: ServerSideEncryption,
                        sys: ActorSystem): CompletionStage[Optional[ObjectMetadata]] =
    S3External.getObjectMetadata(bucket, key, versionId, sse, S3ClientExt.get(sys).client)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
   */
  def deleteObject(bucket: String, key: String, sys: ActorSystem): CompletionStage[Done] =
    deleteObject(bucket, key, Optional.empty(), sys)

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @return A [[java.util.concurrent.CompletionStage CompletionStage]] of [[java.lang.Void]]
   */
  def deleteObject(bucket: String, key: String, versionId: Optional[String], sys: ActorSystem): CompletionStage[Done] =
    S3External.deleteObject(bucket, key, versionId, S3ClientExt.get(sys).client)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                s3Headers: S3Headers,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    S3External.putObject(bucket, key, data, contentLength, contentType, s3Headers, S3ClientExt.get(sys).client)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                s3Headers: S3Headers,
                sse: ServerSideEncryption,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    S3External.putObject(bucket, key, data, contentLength, contentType, s3Headers, sse, S3ClientExt.get(sys).client)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @return ta [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                cannedAcl: CannedAcl,
                metaHeaders: MetaHeaders,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, S3Headers(cannedAcl, metaHeaders), sys)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl the Acl
   * @param metaHeaders the metadata headers
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                cannedAcl: CannedAcl,
                metaHeaders: MetaHeaders,
                sse: ServerSideEncryption,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, S3Headers(cannedAcl, metaHeaders), sse, sys)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, CannedAcl.Private, MetaHeaders(Map()), sys)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                contentType: ContentType,
                sse: ServerSideEncryption,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket, key, data, contentLength, contentType, CannedAcl.Private, MetaHeaders(Map()), sse, sys)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket,
              key,
              data,
              contentLength,
              ContentTypes.APPLICATION_OCTET_STREAM,
              CannedAcl.Private,
              MetaHeaders(Map()),
              sys)

  /**
   * Uploads a S3 Object, use this for small files and [[multipartUpload]] for bigger ones
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param data a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]]
   * @param contentLength the number of bytes that will be uploaded (required!)
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]] of the uploaded S3 Object
   */
  def putObject(bucket: String,
                key: String,
                data: Source[ByteString, _],
                contentLength: Long,
                sse: ServerSideEncryption,
                sys: ActorSystem): CompletionStage[ObjectMetadata] =
    putObject(bucket,
              key,
              data,
              contentLength,
              ContentTypes.APPLICATION_OCTET_STREAM,
              CannedAcl.Private,
              MetaHeaders(Map()),
              sse,
              sys)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               sys: ActorSystem): CompletionStage[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]]] =
    S3External.download(bucket, key, S3ClientExt.get(sys).client)

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      sse: ServerSideEncryption,
      sys: ActorSystem
  ): CompletionStage[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]]] =
    S3External.download(bucket, key, sse, S3ClientExt.get(sys).client)

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String,
               range: ByteRange,
               sys: ActorSystem): CompletionStage[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]]] =
    S3External.download(bucket, key, range, S3ClientExt.get(sys).client)

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      sse: ServerSideEncryption,
      sys: ActorSystem
  ): CompletionStage[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]]] =
    S3External.download(bucket, key, range, sse, S3ClientExt.get(sys).client)

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param versionId optional version id of the object
   * @param sse the server side encryption to use
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      versionId: Optional[String],
      sse: ServerSideEncryption,
      sys: ActorSystem
  ): CompletionStage[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]]] =
    S3External.download(bucket, key, range, versionId, sse, S3ClientExt.get(sys).client)

  /**
   * Will return a source of object metadata for a given bucket with optional prefix using version 2 of the List Bucket API.
   * This will automatically page through all keys with the given parameters.
   *
   * The <code>akka.stream.alpakka.s3.list-bucket-api-version</code> can be set to 1 to use the older API version 1
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html  (version 1 API)
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html (version 1 API)
   *
   * @param bucket Which bucket that you list object metadata for
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String], sys: ActorSystem): Source[ListBucketResultContents, NotUsed] =
    S3External.listBucket(bucket, prefix, S3ClientExt.get(sys).client)

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
                      s3Headers: S3Headers,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    S3External.multipartUpload(bucket, key, contentType, s3Headers, S3ClientExt.get(sys).client)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers,
                      sse: ServerSideEncryption,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    S3External.multipartUpload(bucket, key, contentType, s3Headers, sse, S3ClientExt.get(sys).client)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders), sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param metaHeaders any meta-headers you want to add
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @param sse sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders,
                      sse: ServerSideEncryption,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders), sse, sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()), sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param cannedAcl a [[CannedAcl]], defaults to [[CannedAcl.Private]]
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      sse: ServerSideEncryption,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()), sse, sys)

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
                      contentType: ContentType,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()), sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      sse: ServerSideEncryption,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()), sse, sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()), sys)

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param sse the server side encryption to use
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String,
                      key: String,
                      sse: ServerSideEncryption,
                      sys: ActorSystem): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM, CannedAcl.Private, MetaHeaders(Map()), sse, sys)

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
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    contentType: ContentType,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption,
                    sys: ActorSystem): CompletionStage[MultipartUploadResult] =
    S3External.multipartCopy(sourceBucket,
                             sourceKey,
                             targetBucket,
                             targetKey,
                             sourceVersionId,
                             contentType,
                             s3Headers,
                             sse,
                             S3ClientExt.get(sys).client)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param sourceVersionId version id of source object, if the versioning is enabled in source bucket
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sourceVersionId: Optional[String],
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption,
                    sys: ActorSystem): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  sourceVersionId,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  s3Headers,
                  sse,
                  sys)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param contentType an optional [[akka.http.javadsl.model.ContentType ContentType]]
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    contentType: ContentType,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption,
                    sys: ActorSystem): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, Optional.empty(), contentType, s3Headers, sse, sys)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @param s3Headers any headers you want to add
   * @param sse the server side encryption to use
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    s3Headers: S3Headers,
                    sse: ServerSideEncryption,
                    sys: ActorSystem): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  s3Headers,
                  sse,
                  sys)

  /**
   * Copy a S3 Object by making multiple requests.
   *
   * @param sourceBucket the source s3 bucket name
   * @param sourceKey the source s3 key
   * @param targetBucket the target s3 bucket name
   * @param targetKey the target s3 key
   * @return a [[java.util.concurrent.CompletionStage CompletionStage]] containing the [[akka.stream.alpakka.s3.javadsl.MultipartUploadResult MultipartUploadResult]] of the uploaded S3 Object
   */
  def multipartCopy(sourceBucket: String,
                    sourceKey: String,
                    targetBucket: String,
                    targetKey: String,
                    sys: ActorSystem): CompletionStage[MultipartUploadResult] =
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  S3Headers.empty,
                  null,
                  sys)

}
