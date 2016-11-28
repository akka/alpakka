/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.javadsl

import akka.actor.ActorSystem
import akka.http.impl.model.JavaUri
import akka.http.javadsl.model.Uri
import akka.stream.Materializer
import akka.stream.alpakka.s3.impl.CompleteMultipartUploadResult
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.impl.S3Stream
import akka.stream.alpakka.s3.impl.S3Location
import akka.stream.javadsl.Source
import akka.util.ByteString
import akka.NotUsed
import akka.stream.javadsl.Sink
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def create(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(JavaUri(r.location), r.bucket, r.key, r.etag)
}

final class S3Client(credentials: AWSCredentials, region: String, system: ActorSystem, mat: Materializer) {
  private val impl = new S3Stream(credentials, region)(system, mat)

  def download(bucket: String, key: String): Source[ByteString, NotUsed] =
    impl.download(S3Location(bucket, key)).asJava

  def multipartUpload(bucket: String, key: String): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key))
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava
}
