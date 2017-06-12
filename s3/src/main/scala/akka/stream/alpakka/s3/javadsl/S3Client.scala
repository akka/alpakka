/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.impl.model.JavaUri
import akka.http.javadsl.model.headers.ByteRange
import akka.http.javadsl.model.{ContentType, HttpResponse, Uri}
import akka.http.scaladsl.model.{ContentTypes, ContentType => ScalaContentType}
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.stream.Materializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.impl._
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.compat.java8.FutureConverters._

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def create(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(JavaUri(r.location), r.bucket, r.key, r.etag)
}

object S3Client {
  def create(system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(ConfigFactory.load()), system, mat)

  def create(credentials: AWSCredentials, region: String)(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val settings = S3Settings
      .apply(ConfigFactory.load())
      .copy(awsCredentials = credentials, s3Region = region)
    new S3Client(settings, system, mat)
  }
}

final class S3Client(s3Settings: S3Settings, system: ActorSystem, mat: Materializer) {
  private val impl = S3Stream(s3Settings)(system, mat)

  def request(bucket: String, key: String): CompletionStage[HttpResponse] =
    impl.request(S3Location(bucket, key)).map(_.asInstanceOf[HttpResponse])(system.dispatcher).toJava

  def download(bucket: String, key: String): Source[ByteString, NotUsed] =
    impl.download(S3Location(bucket, key)).asJava

  def download(bucket: String, key: String, range: ByteRange): Source[ByteString, NotUsed] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    impl.download(S3Location(bucket, key), Some(scalaRange)).asJava
  }

  // #list-bucket
  def listBucket(bucket: String, prefix: Option[String]): Source[String, NotUsed] =
    // #list-bucket
    impl.listBucket(bucket, prefix).asJava

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      s3Headers: S3Headers): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key), contentType.asInstanceOf[ScalaContentType], s3Headers)
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl,
                      metaHeaders: MetaHeaders): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, S3Headers(cannedAcl, metaHeaders))

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType,
                      cannedAcl: CannedAcl): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, cannedAcl, MetaHeaders(Map()))

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, contentType, CannedAcl.Private, MetaHeaders(Map()))

  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    multipartUpload(bucket, key, ContentTypes.`application/octet-stream`, CannedAcl.Private, MetaHeaders(Map()))
}
