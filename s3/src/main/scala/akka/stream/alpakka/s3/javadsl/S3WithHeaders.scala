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
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.http.scaladsl.model.{ContentType => ScalaContentType, HttpMethod => ScalaHttpMethod}
import akka.stream.{Attributes, Materializer}
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.impl._
import akka.stream.alpakka.s3._
import akka.stream.javadsl.{RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._

/**
 * Java API
 *
 * Factory of S3 operations with the ability to customize headers sent to S3.
 */
object S3WithHeaders {

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
    request(bucket, key, Optional.empty(), method, s3Headers)

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
    S3Stream
      .request(S3Location(bucket, key),
               method.asInstanceOf[ScalaHttpMethod],
               versionId = Option(versionId.orElse(null)),
               s3Headers = s3Headers.headers)
      .map(v => v: HttpResponse)
      .asJava

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String): Source[Optional[ObjectMetadata], NotUsed] =
    getObjectMetadata(bucket, key, S3Headers())

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String, key: String, s3Headers: S3Headers): Source[Optional[ObjectMetadata], NotUsed] =
    getObjectMetadata(bucket, key, Optional.empty(), s3Headers)

  /**
   * Gets the metadata for a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional versionId of source object
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.javadsl.Source Source]] containing an [[java.util.Optional Optional]] that will be empty in case the object does not exist
   */
  def getObjectMetadata(bucket: String,
                        key: String,
                        versionId: Optional[String],
                        s3Headers: S3Headers): Source[Optional[ObjectMetadata], NotUsed] =
    S3Stream
      .getObjectMetadata(bucket, key, Option(versionId.orElse(null)), s3Headers)
      .map { opt =>
        Optional.ofNullable(opt.orNull)
      }
      .asJava

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObject(bucket: String, key: String): Source[Done, NotUsed] =
    deleteObject(bucket, key, Optional.empty(), S3Headers())

  /**
   * Deletes a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObject(bucket: String,
                   key: String,
                   versionId: Optional[String],
                   s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream
      .deleteObject(S3Location(bucket, key), Option(versionId.orElse(null)), s3Headers)
      .map(_ => Done.getInstance())
      .asJava

  /**
   * Deletes all keys under the specified bucket
   *
   * @param bucket the s3 bucket name
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String): Source[Done, NotUsed] =
    deleteObjectsByPrefix(bucket, Optional.empty(), S3Headers())

  /**
   * Deletes all keys which have the given prefix under the specified bucket
   *
   * @param bucket the s3 bucket name
   * @param prefix optional s3 objects prefix
   * @param s3Headers any headers you want to add
   * @return A [[akka.stream.javadsl.Source Source]] that will emit [[java.lang.Void]] when operation is completed
   */
  def deleteObjectsByPrefix(bucket: String, prefix: Optional[String], s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream
      .deleteObjectsByPrefix(bucket, Option(prefix.orElse(null)), s3Headers)
      .map(_ => Done.getInstance())
      .asJava

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
    S3Stream
      .putObject(S3Location(bucket, key),
                 contentType.asInstanceOf[ScalaContentType],
                 data.asScala,
                 contentLength,
                 s3Headers)
      .asJava

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
    putObject(bucket, key, data, contentLength, contentType, S3Headers().withCannedAcl(CannedAcl.Private))

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
    putObject(bucket, key, data, contentLength, ContentTypes.APPLICATION_OCTET_STREAM)

  private def toJava[M](
      download: akka.stream.scaladsl.Source[Option[
        (akka.stream.scaladsl.Source[ByteString, M], ObjectMetadata)
      ], NotUsed]
  ): Source[Optional[JPair[Source[ByteString, M], ObjectMetadata]], NotUsed] =
    download.map {
      _.map { case (stream, meta) => JPair(stream.asJava, meta) }.asJava
    }.asJava

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(bucket: String,
               key: String): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    toJava(S3Stream.download(S3Location(bucket, key), None, None, S3Headers()))

  /**
   * Downloads a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param s3Headers any headers you want to add
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      s3Headers: S3Headers
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] =
    toJava(
      S3Stream.download(S3Location(bucket, key), None, None, s3Headers)
    )

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
               range: ByteRange): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(S3Stream.download(S3Location(bucket, key), Some(scalaRange), None, S3Headers()))
  }

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param s3Headers any headers you want to add
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      s3Headers: S3Headers
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(
      S3Stream.download(S3Location(bucket, key), Some(scalaRange), None, s3Headers)
    )
  }

  /**
   * Downloads a specific byte range of a S3 Object
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @param range the [[akka.http.javadsl.model.headers.ByteRange ByteRange]] you want to download
   * @param versionId optional version id of the object
   * @param s3Headers any headers you want to add
   * @return A [[akka.japi.Pair]] with a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]], and a [[akka.stream.javadsl.Source Source]] containing the [[ObjectMetadata]]
   */
  def download(
      bucket: String,
      key: String,
      range: ByteRange,
      versionId: Optional[String],
      s3Headers: S3Headers
  ): Source[Optional[JPair[Source[ByteString, NotUsed], ObjectMetadata]], NotUsed] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    toJava(
      S3Stream.download(S3Location(bucket, key), Option(scalaRange), Option(versionId.orElse(null)), s3Headers)
    )
  }

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
  def listBucket(bucket: String,
                 prefix: Option[String],
                 s3Headers: S3Headers): Source[ListBucketResultContents, NotUsed] =
    S3Stream
      .listBucket(bucket, prefix, s3Headers)
      .asJava

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
    S3Stream
      .multipartUpload(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], s3Headers)
      .mapMaterializedValue(_.toJava)
      .asJava

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
    multipartUpload(bucket, key, contentType, S3Headers().withCannedAcl(CannedAcl.Private))

  /**
   * Uploads a S3 Object by making multiple requests
   *
   * @param bucket the s3 bucket name
   * @param key the s3 object key
   * @return a [[akka.stream.javadsl.Sink Sink]] that accepts [[akka.util.ByteString ByteString]]'s and materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[MultipartUploadResult]]
   */
  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.APPLICATION_OCTET_STREAM)

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
    RunnableGraph
      .fromGraph {
        S3Stream
          .multipartCopy(
            S3Location(sourceBucket, sourceKey),
            S3Location(targetBucket, targetKey),
            Option(sourceVersionId.orElse(null)),
            contentType.asInstanceOf[ScalaContentType],
            s3Headers
          )
      }
      .mapMaterializedValue(func(_.toJava))

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
    multipartCopy(sourceBucket,
                  sourceKey,
                  targetBucket,
                  targetKey,
                  sourceVersionId,
                  ContentTypes.APPLICATION_OCTET_STREAM,
                  s3Headers)

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
    multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, Optional.empty(), contentType, s3Headers)

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
    multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, ContentTypes.APPLICATION_OCTET_STREAM, s3Headers)

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
    multipartCopy(sourceBucket, sourceKey, targetBucket, targetKey, ContentTypes.APPLICATION_OCTET_STREAM, S3Headers())

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @param s3Headers any headers you want to add
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String,
                 materializer: Materializer,
                 attributes: Attributes,
                 s3Headers: S3Headers): CompletionStage[Done] =
    S3Stream.makeBucket(bucketName, s3Headers)(materializer, attributes).toJava

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucket(bucketName: String, materializer: Materializer): CompletionStage[Done] =
    S3Stream.makeBucket(bucketName, S3Headers())(materializer, Attributes()).toJava

  /**
   * Create new bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
   * @param bucketName bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.javadsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def makeBucketSource(bucketName: String, s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.makeBucketSource(bucketName, s3Headers).asJava

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @param s3Headers any headers you want to add
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String,
                   materializer: Materializer,
                   attributes: Attributes,
                   s3Headers: S3Headers): CompletionStage[Done] =
    S3Stream.deleteBucket(bucketName, s3Headers)(materializer, attributes).toJava

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucket(bucketName: String, materializer: Materializer): CompletionStage[Done] =
    S3Stream.deleteBucket(bucketName, S3Headers())(materializer, Attributes()).toJava

  /**
   * Delete bucket with a given name
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * @param bucketName   bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.javadsl.Source Source]] of type [[Done]] as API doesn't return any additional information
   */
  def deleteBucketSource(bucketName: String, s3Headers: S3Headers): Source[Done, NotUsed] =
    S3Stream.deleteBucketSource(bucketName, s3Headers).asJava

  /**
   * Checks whether the bucket exists and the user has rights to perform the `ListBucket` operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @param attributes attributes to run request with
   * @param s3Headers any headers you want to add
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String,
                          materializer: Materializer,
                          attributes: Attributes,
                          s3Headers: S3Headers): CompletionStage[BucketAccess] =
    S3Stream.checkIfBucketExists(bucketName, s3Headers)(materializer, attributes).toJava

  /**
   * Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @param materializer materializer to run with
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of type [[BucketAccess]]
   */
  def checkIfBucketExists(bucketName: String, materializer: Materializer): CompletionStage[BucketAccess] =
    S3Stream.checkIfBucketExists(bucketName, S3Headers())(materializer, Attributes()).toJava

  /**
   * Checks whether the bucket exits and user has rights to perform ListBucket operation
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * @param bucketName   bucket name
   * @param s3Headers any headers you want to add
   * @return [[akka.stream.javadsl.Source Source]] of type [[BucketAccess]]
   */
  def checkIfBucketExistsSource(bucketName: String, s3Headers: S3Headers): Source[BucketAccess, NotUsed] =
    S3Stream.checkIfBucketExistsSource(bucketName, s3Headers).asJava

  private def func[T, R](f: T => R) = new akka.japi.function.Function[T, R] {
    override def apply(param: T): R = f(param)
  }
}
