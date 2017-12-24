/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.{MemoryBufferType, S3Settings}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider
import org.scalatest.{FlatSpecLike, Matchers, PrivateMethodTester}

class S3StreamSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with PrivateMethodTester {

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

    implicit val settings = new S3Settings(MemoryBufferType, None, credentialsProvider, regionProvider, false)

    val s3stream = new S3Stream(settings)
    val result: HttpRequest = s3stream invokePrivate requestHeaders(getDownloadRequest(location), None)
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

    implicit val settings = new S3Settings(MemoryBufferType, None, credentialsProvider, regionProvider, false)

    val s3stream = new S3Stream(settings)
    val result: HttpRequest = s3stream invokePrivate requestHeaders(getDownloadRequest(location), Some(range))
    result.headers.size shouldBe 2
    result.headers.seq.exists(_.lowercaseName() == "host")
    result.headers.seq.exists(_.lowercaseName() == "range")

  }
}
