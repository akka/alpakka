/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.MetaHeaders
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  val bucket = "test-bucket"
  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  it should "upload with real credentials" ignore {

    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)
    //val source: Source[ByteString, Any] = FileIO.fromPath(Paths.get("/tmp/IMG_0470.JPG"))

    val result = source.runWith(S3Client().multipartUpload(bucket, objectKey, metaHeaders = MetaHeaders(metaHeaders)))

    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.bucket shouldBe bucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" ignore {

    val download = S3Client().download(bucket, objectKey)

    val result = download.map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe objectValue
  }

  it should "upload and download with spaces in the key" ignore {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val client = S3Client()
    val results = for {
      upload <- source.runWith(S3Client().multipartUpload(bucket, objectKey, metaHeaders = MetaHeaders(metaHeaders)))
      download <- client.download(bucket, objectKey).map(_.decodeString("utf8")).runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = Await.result(results, 10.seconds)

    multipartUploadResult.bucket shouldBe bucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue
  }
}
