/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.stream.alpakka.s3.{BufferType, MemoryBufferType, Proxy, S3Settings}
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

class HttpRequestsSpec extends FlatSpec with Matchers with ScalaFutures {

  // test fixtures
  def getSettings(bufferType: BufferType = MemoryBufferType,
                  diskBufferPath: String = "",
                  proxy: Option[Proxy] = None,
                  awsCredentials: AWSCredentials = AWSCredentials("", ""),
                  s3Region: String = "us-east-1",
                  pathStyleAccess: Boolean = false) =
    new S3Settings(bufferType, diskBufferPath, proxy, awsCredentials, s3Region, pathStyleAccess)

  val location = S3Location("bucket", "image-1024@2x")
  val contentType = MediaTypes.`image/jpeg`
  val acl = CannedAcl.PublicRead
  val metaHeaders: Map[String, String] = Map("location" -> "San Francisco", "orientation" -> "portrait")
  val multipartUpload = MultipartUpload(S3Location("testBucket", "testKey"), "uploadId")

  it should "initiate multipart upload when the region is us-east-1" in {
    implicit val settings = getSettings()

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, S3Headers(acl, MetaHeaders(metaHeaders)))

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/image-1024@2x"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }

  it should "initiate multipart upload with other regions" in {
    implicit val settings = getSettings(s3Region = "us-east-2")

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, S3Headers(acl, MetaHeaders(metaHeaders)))

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3-us-east-2.amazonaws.com"
    req.uri.path.toString shouldEqual "/image-1024@2x"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }

  it should "initiate multipart upload with path-style access in region us-east-1" in {
    implicit val settings = getSettings(s3Region = "us-east-1", pathStyleAccess = true)

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, S3Headers(acl, MetaHeaders(metaHeaders)))

    req.uri.authority.host.toString shouldEqual "s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
  }

  it should "support download requests with path-style access in region us-east-1" in {
    implicit val settings = getSettings(s3Region = "us-east-1", pathStyleAccess = true)

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "s3.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
  }

  it should "initiate multipart upload with path-style access in other regions" in {
    implicit val settings = getSettings(s3Region = "us-west-2", pathStyleAccess = true)

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, S3Headers(acl, MetaHeaders(metaHeaders)))

    req.uri.authority.host.toString shouldEqual "s3-us-west-2.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"

  }

  it should "support download requests with path-style access in other regions" in {
    implicit val settings = getSettings(s3Region = "eu-west-1", pathStyleAccess = true)

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.authority.host.toString shouldEqual "s3-eu-west-1.amazonaws.com"
    req.uri.path.toString shouldEqual "/bucket/image-1024@2x"
  }

  it should "support download requests via HTTP when such scheme configured for `proxy`" in {
    implicit val settings = getSettings(s3Region = "region", proxy = Option(Proxy("localhost", 8080, "http")))

    val req = HttpRequests.getDownloadRequest(location)

    req.uri.scheme shouldEqual "http"
  }

  it should "support multipart init upload requests via HTTP when such scheme configured for `proxy`" in {
    implicit val settings = getSettings(s3Region = "region", proxy = Option(Proxy("localhost", 8080, "http")))

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, S3Headers(acl, MetaHeaders(metaHeaders)))

    req.uri.scheme shouldEqual "http"
  }

  it should "support multipart upload part requests via HTTP when such scheme configured for `proxy`" in {
    implicit val settings = getSettings(s3Region = "region", proxy = Option(Proxy("localhost", 8080, "http")))

    val req =
      HttpRequests.uploadPartRequest(multipartUpload, 1, Source.empty, 1)

    req.uri.scheme shouldEqual "http"
  }

  it should "support multipart upload complete requests via HTTP when such scheme configured for `proxy`" in {
    implicit val settings = getSettings(s3Region = "region", proxy = Option(Proxy("localhost", 8080, "http")))
    implicit val executionContext = scala.concurrent.ExecutionContext.global

    val reqFuture =
      HttpRequests.completeMultipartUploadRequest(multipartUpload, (1, "part") :: Nil)

    reqFuture.futureValue.uri.scheme shouldEqual "http"
  }

  it should "initiate multipart upload with AES-256 server side encryption" in {
    implicit val settings = getSettings(s3Region = "us-east-2", proxy = Option(Proxy("localhost", 8080, "http")))
    val s3Headers = S3Headers(ServerSideEncryption.AES256)
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))
  }

  it should "initiate multipart upload with aws:kms server side encryption" in {
    implicit val settings = getSettings(s3Region = "us-east-2")
    val testArn = "arn:aws:kms:my-region:my-account-id:key/my-key-id"
    val s3Headers = S3Headers(ServerSideEncryption.KMS(testArn))
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    req.headers should contain(RawHeader("x-amz-server-side-encryption-aws-kms-key-id", testArn))
  }

  it should "initiate multipart upload with custom s3 storage class" in {
    implicit val settings = getSettings(s3Region = "us-east-2")
    val s3Headers = S3Headers(StorageClass.ReducedRedundancy)
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("x-amz-storage-class", "REDUCED_REDUNDANCY"))
  }

  it should "initiate multipart upload with custom s3 headers" in {
    implicit val settings = getSettings(s3Region = "us-east-2")
    val s3Headers = S3Headers(Map("Cache-Control" -> "no-cache"))
    val req = HttpRequests.initiateMultipartUploadRequest(location, contentType, s3Headers)

    req.headers should contain(RawHeader("Cache-Control", "no-cache"))
  }

  it should "properly construct the list bucket request with no prefix or continuation token passed" in {
    implicit val settings = getSettings(s3Region = "region", pathStyleAccess = true)

    val req =
      HttpRequests.listBucket(location.bucket)

    req.uri.query() shouldEqual Query("list-type" -> "2")
  }

  it should "properly construct the list bucket request with a prefix and token passed" in {
    implicit val settings = getSettings(s3Region = "region", pathStyleAccess = true)

    val req =
      HttpRequests.listBucket(location.bucket, Some("random/prefix"), Some("randomToken"))

    req.uri.query() shouldEqual Query("list-type" -> "2",
                                      "prefix" -> "random/prefix",
                                      "continuation-token" -> "randomToken")
  }
}
