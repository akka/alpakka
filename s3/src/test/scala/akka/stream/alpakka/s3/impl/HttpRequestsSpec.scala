/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import org.scalatest.Inspectors.{forAll => iforAll}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Seq

class HttpRequestsSpec extends FlatSpec with Matchers {
  it should "initiate multipart upload when the region is us-east-1" in {
    implicit val settings = S3Settings(ActorSystem())

    val location = S3Location("bucket", "image-1024@2x")
    val contentType = MediaTypes.`image/jpeg`
    val acl = CannedAcl.PublicRead
    val metaHeaders: Map[String, String] = Map("location" -> "San Francisco", "orientation" -> "portrait")

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, acl, "us-east-1", MetaHeaders(metaHeaders))

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3.amazonaws.com"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }

  it should "initiate multipart upload with other regions" in {
    implicit val settings = S3Settings(ActorSystem())

    val location = S3Location("bucket", "image-1024@2x")
    val contentType = MediaTypes.`image/jpeg`
    val acl = CannedAcl.PublicRead
    val metaHeaders: Map[String, String] = Map("location" -> "San Francisco", "orientation" -> "portrait")

    val req =
      HttpRequests.initiateMultipartUploadRequest(location, contentType, acl, "us-east-2", MetaHeaders(metaHeaders))

    req.entity shouldEqual HttpEntity.empty(contentType)
    req.headers should contain(RawHeader("x-amz-acl", acl.value))
    req.uri.authority.host.toString shouldEqual "bucket.s3-us-east-2.amazonaws.com"

    metaHeaders.map { m =>
      req.headers should contain(RawHeader(s"x-amz-meta-${m._1}", m._2))
    }
  }
}
