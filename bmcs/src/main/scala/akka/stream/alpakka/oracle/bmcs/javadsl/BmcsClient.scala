/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.javadsl

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.javadsl.model.headers.ByteRange
import akka.http.javadsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{ByteRange => ScalaByteRange}
import akka.stream.Materializer
import akka.stream.alpakka.oracle.bmcs.BmcsSettings
import akka.stream.alpakka.oracle.bmcs.auth.BmcsCredentials
import akka.stream.alpakka.oracle.bmcs.impl.{BmcsStream, CompleteMultipartUploadResult, ObjectSummary}
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

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
  def create(obj: ObjectSummary, bucket: String): ListObjectsResultContents =
    ListObjectsResultContents(obj.name, bucket, obj.size, obj.md5, obj.timeCreated)
}

object MultipartUploadResult {
  def create(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.bucket, r.objectName, r.etag)
}

object BmcsClient {
  def create(credentials: BmcsCredentials, system: ActorSystem, mat: Materializer): BmcsClient =
    new BmcsClient(BmcsSettings(ConfigFactory.load()), credentials, system, mat)
}

final class BmcsClient(val settings: BmcsSettings, val cred: BmcsCredentials, system: ActorSystem, mat: Materializer) {

  private[this] val impl = BmcsStream(settings, cred)(system, mat)

  def request(bucket: String, objectName: String): CompletionStage[HttpResponse] =
    impl.request(bucket, objectName).map(_.asInstanceOf[HttpResponse])(system.dispatcher).toJava

  def download(bucket: String, objectName: String): Source[ByteString, NotUsed] =
    impl.download(bucket, objectName).asJava

  def download(bucket: String, objectName: String, range: ByteRange): Source[ByteString, NotUsed] = {
    val scalaRange = range.asInstanceOf[ScalaByteRange]
    impl.download(bucket, objectName, Some(scalaRange)).asJava
  }

  /**
   * Will return a source of object metadata for a given bucket with optional prefix.
   * This will automatically page through all keys with the given parameters.
   *
   * @param prefix Prefix of the keys you want to list under passed bucket
   * @return Source of object metadata
   */
  def listBucket(bucket: String, prefix: Option[String] = None): Source[ListObjectsResultContents, NotUsed] =
    impl
      .listBucket(bucket, prefix = prefix)
      .map { scalaContents =>
        ListObjectsResultContents(scalaContents.name,
                                  scalaContents.bucket,
                                  scalaContents.size,
                                  scalaContents.md5,
                                  scalaContents.timeCreated)
      }
      .asJava

  def multipartUpload(bucket: String,
                      objectName: String,
                      chunkSize: Int,
                      chunkingParallelism: Int = 4): Sink[ByteString, CompletionStage[MultipartUploadResult]] =
    impl
      .multipartUpload(bucket, objectName, chunkSize, chunkingParallelism)
      .mapMaterializedValue(_.map(MultipartUploadResult.create)(system.dispatcher).toJava)
      .asJava

}
