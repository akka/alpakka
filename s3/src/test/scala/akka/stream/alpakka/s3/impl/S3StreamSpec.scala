/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}
import akka.stream.alpakka.s3.{MemoryBufferType, S3Settings}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import org.scalatest.{FlatSpecLike, Matchers, PrivateMethodTester}

class S3StreamSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with PrivateMethodTester {

  import HttpRequests._

  def this() = this(ActorSystem("S3StreamSpec"))
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  "S3Stream" should "have an empty headers when call downloadOrUpdatePart without server side encryption" in {
    val credentials =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3Stream = new S3Stream(settings)
    val result = s3Stream.downloadOrUpdatePartHeaders(None)

    result.headers.size shouldBe 0
  }

  it should "have empty header when call downloadOrUpdatePart with AE256 server side encryption" in {
    val credentials =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3Stream = new S3Stream(settings)
    val sse = Some(ServerSideEncryption.AES256)
    val result = s3Stream.downloadOrUpdatePartHeaders(sse)

    result.headers.size shouldBe 0
  }

  it should "have empty header when call downloadOrUpdatePart with KMS server side encryption" in {
    val credentials =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3Stream = new S3Stream(settings)
    val sse = Some(ServerSideEncryption.KMS("my-key"))
    val result = s3Stream.downloadOrUpdatePartHeaders(sse)

    result.headers.size shouldBe 0
  }

  it should "have 3 headers when call downloadOrUpdatePart with Customer key server side encryption" in {
    val credentials =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3Stream = new S3Stream(settings)
    val sse = Some(ServerSideEncryption.CustomerKeys("my-key", Some("md5")))
    val result = s3Stream.downloadOrUpdatePartHeaders(sse)

    result.headers.size shouldBe 3
    result.headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    result.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", "my-key"))
    result.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", "md5"))
  }

  "Non-ranged downloads" should "have one (host) header" in {

    val requestHeaders = PrivateMethod[HttpRequest]('requestHeaders)
    val credentials = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(
        "test-Id",
        "test-key"
      )
    )
    val location = S3Location("test-bucket", "test-key")

    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3stream = new S3Stream(settings)
    val result: HttpRequest = s3stream invokePrivate requestHeaders(getDownloadRequest(location), None)
    result.headers.size shouldBe 1
    result.headers.seq.exists(_.lowercaseName() == "host")
  }

  "Ranged downloads" should "have two (host, range) headers" in {

    val requestHeaders = PrivateMethod[HttpRequest]('requestHeaders)
    val credentials =
      new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          "test-Id",
          "test-key"
        )
      )
    val location = S3Location("test-bucket", "test-key")
    val range = ByteRange(1, 4)

    implicit val settings = new S3Settings(MemoryBufferType, None, credentials, "us-east-1", false)

    val s3stream = new S3Stream(settings)
    val result: HttpRequest = s3stream invokePrivate requestHeaders(getDownloadRequest(location), Some(range))
    result.headers.size shouldBe 2
    result.headers.seq.exists(_.lowercaseName() == "host")
    result.headers.seq.exists(_.lowercaseName() == "range")

  }
}
