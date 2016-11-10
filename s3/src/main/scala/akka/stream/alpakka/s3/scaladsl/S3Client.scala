package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.s3.impl.S3Location
import akka.stream.alpakka.s3.impl.S3Stream
import akka.stream.alpakka.s3.signing.AWSCredentials
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.http.scaladsl.model.Uri
import akka.stream.scaladsl.Sink
import scala.concurrent.Future
import akka.stream.alpakka.s3.impl.CompleteMultipartUploadResult

case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult) = new MultipartUploadResult(r.location, r.bucket, r.key, r.etag)
}

object S3Client {
  val MIN_CHUNK_SIZE = 5242880
}

class S3Client(credentials: AWSCredentials, region: String)(implicit system: ActorSystem, mat: Materializer) {
  import S3Client._
  private val impl = new S3Stream(credentials, region)

  def download(bucket: String, key: String): Source[ByteString, NotUsed] = impl.download(S3Location(bucket, key))

  def multipartUpload(bucket: String, key: String, chunkSize: Int = MIN_CHUNK_SIZE, chunkingParallelism: Int = 4): Sink[ByteString, Future[MultipartUploadResult]] =
    impl.multipartUpload(S3Location(bucket, key), chunkSize, chunkingParallelism).mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))
}
