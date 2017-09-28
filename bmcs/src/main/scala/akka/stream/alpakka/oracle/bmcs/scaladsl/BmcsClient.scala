/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpResponse}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Materializer
import akka.stream.alpakka.oracle.bmcs.BmcsSettings
import akka.stream.alpakka.oracle.bmcs.auth.BmcsCredentials
import akka.stream.alpakka.oracle.bmcs.impl._
import akka.stream.alpakka.s3.scaladsl.S3Client.MinChunkSize
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

final case class MultipartUploadResult(bucketName: String, objectName: String, etag: String)

final case class ListObjectsResultContents(
    /** the name of the object in bmcs **/
    name: String,
    /** the name of the bucket in which this object is stored. **/
    bucket: String,
    /** the size of the object,  in bytes **/
    size: Option[Long],
    /** Base64 encoded MD5 hash of the object Data.  **/
    md5: Option[String],
    /** The date and time the object was created, as described in RFC 2616, section 14.29. **/
    timeCreated: Option[String]
)

object ListObjectsResultContents {
  def apply(obj: ObjectSummary, bucket: String): ListObjectsResultContents =
    ListObjectsResultContents(obj.name, bucket, obj.size, obj.md5, obj.timeCreated)
}

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.bucket, r.objectName, r.etag)
}

final class BmcsClient(val settings: BmcsSettings, val cred: BmcsCredentials)(implicit system: ActorSystem,
                                                                              mat: Materializer) {

  private[this] val impl = BmcsStream(settings, cred)

  def request(bucket: String, objectName: String): Future[HttpResponse] =
    impl.request(bucket, objectName)

  def download(bucket: String, objectName: String): Source[ByteString, NotUsed] =
    impl.download(bucket, objectName)

  def download(bucket: String, objectName: String, range: ByteRange): Source[ByteString, NotUsed] =
    impl.download(bucket, objectName, Some(range))

  /**
   * Will return a source of object metadata for a given bucket with optional prefix.
   * This will automatically page through all keys with the given parameters.
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String] = None): Source[ListObjectsResultContents, NotUsed] =
    impl.listBucket(bucket, prefix = prefix)

  def multipartUpload(bucket: String,
                      objectName: String,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(bucket, objectName, chunkSize, chunkingParallelism)
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))

}
