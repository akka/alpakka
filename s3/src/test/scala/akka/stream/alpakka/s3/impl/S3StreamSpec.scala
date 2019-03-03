/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.{ApiVersion, MemoryBufferType, S3Settings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import akka.util.ByteString
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FlatSpecLike, Matchers, PrivateMethodTester}

class S3StreamSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with PrivateMethodTester
    with ScalaFutures
    with IntegrationPatience {

  import HttpRequests._

  def this() = this(ActorSystem("S3StreamSpec"))
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  "Non-ranged downloads" should "have one (host) header" in {

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
      S3Settings(MemoryBufferType,
                 None,
                 credentialsProvider,
                 regionProvider,
                 false,
                 None,
                 ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), None)
    result.headers.size shouldBe 1
    result.headers.seq.exists(_.lowercaseName() == "host")
  }

  "Ranged downloads" should "have two (host, range) headers" in {

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
      S3Settings(MemoryBufferType,
                 None,
                 credentialsProvider,
                 regionProvider,
                 false,
                 None,
                 ApiVersion.ListBucketVersion2)

    val result: HttpRequest = S3Stream invokePrivate requestHeaders(getDownloadRequest(location), Some(range))
    result.headers.size shouldBe 2
    result.headers.seq.exists(_.lowercaseName() == "host")
    result.headers.seq.exists(_.lowercaseName() == "range")

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

}
