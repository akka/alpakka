/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.impl._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.location, r.bucket, r.key, r.etag)
}

object S3Client {
  val MinChunkSize = 5242880

  def apply()(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val s3Settings = S3Settings(system)
    new S3Client(s3Settings.awsCredentials, s3Settings.s3Region)
  }
}

final class S3Client(credentials: AWSCredentials, region: String)(implicit system: ActorSystem, mat: Materializer) {

  import S3Client._

  private[this] val impl = S3Stream(credentials, region)

  def request(bucket: String, key: String): Future[HttpResponse] = impl.request(S3Location(bucket, key))

  def download(bucket: String, key: String): Source[ByteString, NotUsed] = impl.download(S3Location(bucket, key))

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType = ContentTypes.`application/octet-stream`,
                      metaHeaders: MetaHeaders = MetaHeaders(Map()),
                      cannedAcl: CannedAcl = CannedAcl.Private,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4,
                      amzHeaders: Option[AmzHeaders] = None): Sink[ByteString, Future[MultipartUploadResult]] = {

    val s3Headers = S3Headers(
      cannedAcl,
      metaHeaders,
      amzHeaders
    )

    impl
      .multipartUpload(
        S3Location(bucket, key),
        contentType,
        s3Headers,
        chunkSize,
        chunkingParallelism
      )
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

  }
}
