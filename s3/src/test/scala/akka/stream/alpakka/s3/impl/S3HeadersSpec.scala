/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.alpakka.s3.{MetaHeaders, S3Headers}
import akka.stream.alpakka.s3.headers.{CannedAcl, ServerSideEncryption, StorageClass}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3HeadersSpec extends AnyFlatSpec with Matchers {

  "ServerSideEncryption" should "create well formed headers for AES-256 encryption" in {
    ServerSideEncryption.aes256().headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))
  }

  it should "create well formed headers for AES-256 encryption by requests" in {
    var headers = ServerSideEncryption.aes256().headersFor(HeadObject)
    headers.size shouldBe 0

    headers = ServerSideEncryption.aes256().headersFor(GetObject)
    headers.size shouldBe 0

    headers = ServerSideEncryption.aes256().headersFor(PutObject)
    headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))

    headers = ServerSideEncryption.aes256().headersFor(InitiateMultipartUpload)
    headers should contain(RawHeader("x-amz-server-side-encryption", "AES256"))

    headers = ServerSideEncryption.aes256().headersFor(UploadPart)
    headers.size shouldBe 0
  }

  it should "create well formed headers for aws:kms encryption" in {
    val kms =
      ServerSideEncryption
        .kms("arn:aws:kms:my-region:my-account-id:key/my-key-id")
        .withContext("base-64-encoded-context")
    kms.headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    kms.headers should contain(
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:my-region:my-account-id:key/my-key-id")
    )
    kms.headers should contain(RawHeader("x-amz-server-side-encryption-context", "base-64-encoded-context"))
  }

  it should "create well formed headers for aws:kms encryption by requests" in {
    val kms =
      ServerSideEncryption
        .kms("arn:aws:kms:my-region:my-account-id:key/my-key-id")
        .withContext("base-64-encoded-context")

    var headers = kms.headersFor(HeadObject)
    headers.size shouldBe 0

    headers = kms.headersFor(GetObject)
    headers.size shouldBe 0

    headers = kms.headersFor(PutObject)
    headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    headers should contain(
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:my-region:my-account-id:key/my-key-id")
    )
    headers should contain(RawHeader("x-amz-server-side-encryption-context", "base-64-encoded-context"))

    headers = kms.headersFor(InitiateMultipartUpload)
    headers should contain(RawHeader("x-amz-server-side-encryption", "aws:kms"))
    headers should contain(
      RawHeader("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:my-region:my-account-id:key/my-key-id")
    )
    headers should contain(RawHeader("x-amz-server-side-encryption-context", "base-64-encoded-context"))

    headers = kms.headersFor(UploadPart)
    headers.size shouldBe 0
  }

  it should "create well formed headers for customer keys encryption" in {
    val key = "rOJ7HxUze312HtqOL+55ahqbokC+nc614oRlrYdjGhE="
    val md5Key = "nU0sHdKlctQdn+Up4POVJw=="

    var ssec = ServerSideEncryption.customerKeys(key).withMd5(md5Key)
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    //Non md5
    ssec = ServerSideEncryption.customerKeys(key)
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    ssec.headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))
  }

  it should "create well formed headers for customer keys encryption by requests" in {
    val key = "rOJ7HxUze312HtqOL+55ahqbokC+nc614oRlrYdjGhE="
    val md5Key = "nU0sHdKlctQdn+Up4POVJw=="

    val ssec = ServerSideEncryption.customerKeys(key).withMd5(md5Key)

    var headers = ssec.headersFor(HeadObject)
    headers.size shouldBe 3
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    headers = ssec.headersFor(GetObject)
    headers.size shouldBe 3
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    headers = ssec.headersFor(PutObject)
    headers.size shouldBe 3
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    headers = ssec.headersFor(InitiateMultipartUpload)
    headers.size shouldBe 3
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    headers = ssec.headersFor(UploadPart)
    headers.size shouldBe 3
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))

    headers = ssec.headersFor(CopyPart)
    headers.size shouldBe 6
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-server-side-encryption-customer-key-MD5", md5Key))
    headers should contain(RawHeader("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256"))
    headers should contain(RawHeader("x-amz-copy-source-server-side-encryption-customer-key", key))
    headers should contain(RawHeader("x-amz-copy-source-server-side-encryption-customer-key-MD5", md5Key))
  }

  "StorageClass" should "create well formed headers for 'infrequent access'" in {
    StorageClass.InfrequentAccess.header shouldEqual RawHeader("x-amz-storage-class", "STANDARD_IA")
  }

  "S3Headers" should "aggregate headers to one sequence" in {
    val s3Headers = S3Headers()
      .withCannedAcl(CannedAcl.PublicRead)
      .withMetaHeaders(MetaHeaders(Map("custom-meta" -> "custom")))
      .withCustomHeaders(Map("Cache-Control" -> "no-cache"))
      .withStorageClass(StorageClass.ReducedRedundancy)

    s3Headers.headers.size shouldEqual 4
  }
}
