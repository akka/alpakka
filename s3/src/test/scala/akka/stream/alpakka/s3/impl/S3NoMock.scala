/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.alpakka.s3.scaladsl.S3Client
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 *
 * @author siva
 */
class S3NoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val bucket = "test-bucket"
  val objectKey = "test"
  val objectValue = "Some content."

  it should "upload with real credentials" ignore {

    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val result = source.runWith(S3Client().multipartUpload(bucket, objectKey, contentType = ContentTypes.`text/plain(UTF-8)`))

    val multipartUploadResult = Await.ready(result, 5.seconds).futureValue
    multipartUploadResult.bucket shouldBe bucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" ignore {

    val download = S3Client().download(bucket, objectKey)

    val result = download.map(_.decodeString("utf8")).runWith(Sink.head)

    Await.ready(result, 5.seconds).futureValue shouldBe objectValue
  }
}
