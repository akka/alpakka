/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.alpakka.s3.BucketAccess.{AccessDenied, AccessGranted, NotExists}
import akka.stream.alpakka.s3.{ApiVersion, BucketAccess, MemoryBufferType, S3Settings}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Attributes, SystemMaterializer}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers._

import scala.concurrent.Future

class S3StreamSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with PrivateMethodTester
    with ScalaFutures
    with IntegrationPatience
    with LogCapturing {

  import HttpRequests._

  def this() = this(ActorSystem("S3StreamSpec"))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "Non-ranged downloads" should "have two (host and synthetic raw-request-uri) headers" in {

    val requestHeaders = PrivateMethod[HttpRequest](Symbol("requestHeaders"))
    val credentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(
        "test-Id",
        "test-key"
      )
    )
    val regionProvider = new AwsRegionProvider {
      def getRegion = Region.US_EAST_1
    }
    val location = S3Location("test-bucket", "test-key")

    implicit val settings =
      S3Settings(MemoryBufferType, credentialsProvider, regionProvider, ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), None)
    result.headers.size shouldBe 2
    result.headers.exists(_.lowercaseName() == "host")
    result.headers.exists(_.lowercaseName() == "raw-request-uri")
  }

  "Ranged downloads" should "have three (host, range and synthetic raw-request-uri) headers" in {

    val requestHeaders = PrivateMethod[HttpRequest](Symbol("requestHeaders"))
    val credentialsProvider =
      StaticCredentialsProvider.create(
        AwsBasicCredentials.create(
          "test-Id",
          "test-key"
        )
      )
    val regionProvider =
      new AwsRegionProvider {
        def getRegion: Region = Region.US_EAST_1
      }
    val location = S3Location("test-bucket", "test-key")
    val range = ByteRange(1, 4)

    implicit val settings =
      S3Settings(MemoryBufferType, credentialsProvider, regionProvider, ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), Some(range))
    result.headers.size shouldBe 3
    result.headers.exists(_.lowercaseName() == "host")
    result.headers.exists(_.lowercaseName() == "range")
    result.headers.exists(_.lowercaseName() == "raw-request-uri")

  }

  it should "properly handle input streams, even when empty" in {
    def nonEmptySrc = Source.repeat(ByteString("hello world"))

    nonEmptySrc
      .take(1)
      .via(S3Stream.atLeastOneByteString)
      .toMat(Sink.seq[ByteString])(Keep.right)
      .run()
      .futureValue should equal(Seq(ByteString("hello world")))

    nonEmptySrc
      .take(10)
      .via(S3Stream.atLeastOneByteString)
      .toMat(Sink.seq[ByteString])(Keep.right)
      .run()
      .futureValue should equal(Seq.fill(10)(ByteString("hello world")))

    nonEmptySrc
      .take(0)
      .via(S3Stream.atLeastOneByteString)
      .toMat(Sink.seq[ByteString])(Keep.right)
      .run()
      .futureValue should equal(Seq(ByteString.empty))
  }

  it should "create partitions when object size is not multiple of chunk size" in {
    val chunkSize = 25
    val objectSize = 69L
    val sourceLocation = S3Location("test-bucket", "test-key")

    val partitions: List[CopyPartition] = S3Stream.createPartitions(chunkSize, sourceLocation)(objectSize)
    partitions should have length 3
    partitions should equal(
      List(
        CopyPartition(1, sourceLocation, Some(ByteRange(0, 25))),
        CopyPartition(2, sourceLocation, Some(ByteRange(25, 50))),
        CopyPartition(3, sourceLocation, Some(ByteRange(50, 69)))
      )
    )
  }

  it should "create partitions when object size is multiple of chunk size" in {
    val chunkSize = 25
    val objectSize = 50L
    val sourceLocation = S3Location("test-bucket", "test-key")

    val partitions: List[CopyPartition] = S3Stream.createPartitions(chunkSize, sourceLocation)(objectSize)
    partitions should have length 2
    partitions should equal(
      List(CopyPartition(1, sourceLocation, Some(ByteRange(0, 25))),
           CopyPartition(2, sourceLocation, Some(ByteRange(25, 50))))
    )
  }

  it should "partition with object size is equal to 0 should not contain range info" in {
    val chunkSize = 25
    val objectSize = 0L
    val sourceLocation = S3Location("test-bucket", "test-key")

    val partitions: List[CopyPartition] = S3Stream.createPartitions(chunkSize, sourceLocation)(objectSize)
    partitions should have length 1
    partitions should equal(List(CopyPartition(1, sourceLocation)))
  }

  "processCheckIfExistsResponse" should "convert head response to BucketAccess" in {
    def bucketStatusPreparation(response: HttpResponse): Future[BucketAccess] = {
      val testedMethod = PrivateMethod[Future[BucketAccess]](Symbol("processCheckIfExistsResponse"))

      val result: Future[BucketAccess] = S3Stream invokePrivate testedMethod(response,
                                                                             SystemMaterializer(system).materializer)

      result
    }

    val responseWithOkCode = HttpResponse(
      status = StatusCodes.OK
    )

    bucketStatusPreparation(responseWithOkCode).futureValue shouldEqual AccessGranted

    val responseWithNotFoundCode = HttpResponse(
      status = StatusCodes.NotFound
    )

    bucketStatusPreparation(responseWithNotFoundCode).futureValue shouldEqual NotExists

    val responseWithForbiddenCode = HttpResponse(
      status = StatusCodes.Forbidden
    )

    bucketStatusPreparation(responseWithForbiddenCode).futureValue shouldEqual AccessDenied
  }

  it should "only resolve the default S3 settings once per actor system" in {

    val attr = Attributes()
    val resolveSettings = PrivateMethod[S3Settings](Symbol("resolveSettings"))
    val settings1 = S3Stream invokePrivate resolveSettings(attr, system)
    val settings2 = S3Stream invokePrivate resolveSettings(attr, system)

    settings1 eq settings2 shouldBe true
  }
}
