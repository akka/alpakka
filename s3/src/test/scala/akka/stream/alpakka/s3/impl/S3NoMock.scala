/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.scaladsl.S3Client
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()

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

  it should "list contents of file " ignore {
    val region = "us-west-2"
    val client = new S3Stream(
      AWSCredentials("", ""),
      region = region,
      settings = S3Settings(actorSystem)
    )
    val a = client.listBucket("salesreports").runWith(Sink.foreach(println))
    Await.ready(a, 20.seconds).futureValue shouldBe (akka.Done)
  }
}
