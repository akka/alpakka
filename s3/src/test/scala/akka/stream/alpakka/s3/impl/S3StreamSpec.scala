/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.BucketAccess.{AccessDenied, AccessGranted, NotExists}
import akka.stream.alpakka.s3.{ApiVersion, BucketAccess, MemoryBufferType, S3Settings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import akka.util.ByteString
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers, PrivateMethodTester}

import scala.concurrent.Future

class S3StreamSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with PrivateMethodTester
    with ScalaFutures
    with IntegrationPatience {

  import HttpRequests._

  def this() = this(ActorSystem("S3StreamSpec"))
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "Non-ranged downloads" should "have two (host and synthetic raw-request-uri) headers" in {

    val requestHeaders = PrivateMethod[HttpRequest]('requestHeaders)
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(
        "test-Id",
        "test-key"
      )
    )
    val regionProvider = new AwsRegionProvider {
      def getRegion = "us-east-1"
    }
    val location = S3Location("test-bucket", "test-key")

    implicit val settings =
      S3Settings(MemoryBufferType, credentialsProvider, regionProvider, ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), None)
    result.headers.size shouldBe 2
    result.headers.seq.exists(_.lowercaseName() == "host")
    result.headers.seq.exists(_.lowercaseName() == "raw-request-uri")
  }

  "Ranged downloads" should "have three (host, range and synthetic raw-request-uri) headers" in {

    val requestHeaders = PrivateMethod[HttpRequest]('requestHeaders)
    val credentialsProvider =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    val regionProvider =
      new AwsRegionProvider {
        def getRegion: String = "us-east-1"
      }
    val location = S3Location("test-bucket", "test-key")
    val range = ByteRange(1, 4)

    implicit val settings =
      S3Settings(MemoryBufferType, credentialsProvider, regionProvider, ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), Some(range))
    result.headers.size shouldBe 3
    result.headers.seq.exists(_.lowercaseName() == "host")
    result.headers.seq.exists(_.lowercaseName() == "range")
    result.headers.seq.exists(_.lowercaseName() == "raw-request-uri")

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
      val testedMethod = PrivateMethod[Future[BucketAccess]]('processCheckIfExistsResponse)

      val result: Future[BucketAccess] = S3Stream invokePrivate testedMethod(response, materializer)

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
}
