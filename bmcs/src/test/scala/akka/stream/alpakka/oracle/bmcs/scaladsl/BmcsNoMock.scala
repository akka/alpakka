/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.scaladsl

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.oracle.bmcs.{BmcsSettings, MemoryBufferType, TestUtil}
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore, Matchers}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import akka.stream.alpakka.oracle.bmcs.auth._

/*
 * This is an integration test and ignored by default
 *
 * For running the tests you need to create 1 bucket:
 * Update the bucket name and regions in the code below
 *
 * update the ocids and keys.
 *
 * Comment @ignore and run the tests
 *
 */
@Ignore
class BmcsNoMock extends FlatSpecLike with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = materializer.executionContext

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  val userOcid = "ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq"
  val keyFingerprint = "cb:17:e0:45:d0:24:d3:ff:be:ee:1b:0e:f8:2c:58:27"
  val tenancyOcid = "ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa"
  val keyPath = "SOME_KEY_PATH"
  val passphrase: Option[String] = Some("aditya")
  val cred = BasicCredentials(userOcid, tenancyOcid, keyPath, passphrase, keyFingerprint)
  val bucket = "CEGBU_Prime"

  val settings =
    BmcsSettings().copy(bufferType = MemoryBufferType, region = "us-phoenix-1", namespace = "oraclegbudev")
  val client = BmcsClient(cred, settings)
  val largeSrc = TestUtil.largeSource()

  "BmcsClient " should "upload a document " in {

    val objectName = s"BmcsNoMockTest-${UUID.randomUUID().toString.take(5)}"
    val sink = client.multipartUpload(bucket, objectName)

    val result = largeSrc.runWith(sink)
    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.etag shouldNot be("")

    //now download the same object.
    //compare the hashes of the upload and download.
    val download = client.download(bucket, objectName)

    val hashOfDownload = download.runWith(digest())
    val hashOfUpload = largeSrc.runWith(digest())

    val uploadHash = Await.ready(hashOfUpload, 20.seconds).futureValue
    val downloadHash = Await.ready(hashOfDownload, 20.seconds).futureValue
    uploadHash should equal(downloadHash)
  }

  "it" should "list objects in the bucket correctly " in {
    val objects: Source[ListObjectsResultContents, NotUsed] = client.listBucket(bucket)
    val countFuture: Future[Int] = objects.map(_ => 1).runWith(Sink.fold(0)(_ + _))
    val count = Await.ready(countFuture, 20.seconds).futureValue
    count should be > 0
  }

}
