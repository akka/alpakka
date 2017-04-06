/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.model.headers.RawHeader
import org.scalatest.{FlatSpec, Matchers}

class S3HeadersSpec extends FlatSpec with Matchers {

  "ServerSideEncryption" should "create well formed headers for AES-256 encryption" in {
    ServerSideEncryption.AES256.headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))
  }

  it should "create well formed headers for aws:kms encryption" in {
    val kms =
      ServerSideEncryption.KMS("arn:aws:kms:my-region:my-account-id:key/my-key-id", Some("base-64-encoded-context"))
    kms.headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    kms.headers should contain(
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:my-region:my-account-id:key/my-key-id")
    )
    kms.headers should contain(RawHeader("x-amz-server-side-encryption-context", "base-64-encoded-context"))
  }

  "StorageClass" should "create well formed headers for 'infrequent access'" in {
    StorageClass.InfrequentAccess.header shouldEqual RawHeader("x-amz-storage-class", "STANDARD_IA")
  }

  "AmzHeaders" should "have aggregate headers to one sequence" in {
    val amz = AmzHeaders(ServerSideEncryption.AES256,
                         Seq(RawHeader("Cache-Control", "no-cache")),
                         StorageClass.ReducedRedundancy)
    amz.headers.size shouldEqual 3
  }
}
