/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.scaladsl

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.oracle.bmcs.BmcsSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
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

  val settings: BmcsSettings = BmcsSettings(ConfigFactory.load())

  val userOcid = "ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq"
  val keyFingerprint = "b4:97:75:d0:2a:40:1f:5e:66:a3:f4:03:9a:ff:a8:8a"
  val tenantcyOcid = "ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa"
  val keyPath = "./bmcs/src/test/resources/oci_api_key.pem"
  val passphrase: Option[String] = Some("adityag")
  val cred = BmcsCredentials(userOcid, tenantcyOcid, keyPath, passphrase, keyFingerprint)
  val bucket = "CEGBU_Prime"

  val client = new BmcsClient(settings, cred)

  val largeString: String = (0 to 9).map(_.toString * 1024).fold("")(_ + _)
  val largeSrc: Source[ByteString, NotUsed] =
    Source.fromIterator(() => (0 to 1).iterator).map(_ => ByteString(largeString))

  "BmcsClient " should "upload a document " in {
    val objectName = s"BmcsNoMockTest-${UUID.randomUUID().toString.take(5)}"
    val sink = client.multipartUpload(bucket, objectName)
    val result: Future[MultipartUploadResult] = largeSrc.runWith(sink)
    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    multipartUploadResult.etag shouldNot be("")

    //now download the same object.
    //compare the hashes of the upload and download.
    val download: Source[ByteString, NotUsed] = client.download(bucket, objectName)

    val hashOfDownload: Future[ByteString] = download.runWith(digest())
    val hashOfUpload: Future[ByteString] = largeSrc.runWith(digest())

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
