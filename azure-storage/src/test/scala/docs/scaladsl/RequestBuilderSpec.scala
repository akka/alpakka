/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.requests.{CreateFile, GetBlob, PutBlockBlob}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RequestBuilderSpec extends AnyFlatSpec with Matchers with LogCapturing {

  it should "create request builder with default values" in {

    //#simple-request-builder
    val requestBuilder = GetBlob()
    //#simple-request-builder

    requestBuilder.versionId shouldBe empty
    requestBuilder.range shouldBe empty
    requestBuilder.leaseId shouldBe empty
    requestBuilder.sse shouldBe empty
    requestBuilder.additionalHeaders shouldBe empty
  }

  it should "create request builder with values" in {

    //#populate-request-builder
    val requestBuilder = GetBlob().withLeaseId("my-lease-id").withRange(ByteRange(0, 25))
    //#populate-request-builder

    requestBuilder.leaseId shouldBe Some("my-lease-id")
    requestBuilder.range shouldBe Some(ByteRange(0, 25))
    requestBuilder.sse shouldBe empty
  }

  it should "create request builder with initial values" in {

    //#request-builder-with-initial-values
    val requestBuilder = CreateFile(256L, ContentTypes.`text/plain(UTF-8)`)
    //#request-builder-with-initial-values

    requestBuilder.leaseId shouldBe empty
    requestBuilder.maxFileSize shouldBe 256L
    requestBuilder.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
  }

  it should "populate request builder with ServerSideEncryption" in {

    //#request-builder-with-sse
    val requestBuilder = PutBlockBlob(256L, ContentTypes.`text/plain(UTF-8)`)
      .withServerSideEncryption(ServerSideEncryption.customerKey("SGVsbG9Xb3JsZA=="))
    //#request-builder-with-sse

    requestBuilder.sse shouldBe Some(ServerSideEncryption.customerKey("SGVsbG9Xb3JsZA=="))
  }

  it should "populate request builder with additional headers" in {

    //#request-builder-with-additional-headers
    val requestBuilder = GetBlob().addHeader("If-Match", "foobar")
    //#request-builder-with-additional-headers

    requestBuilder.additionalHeaders shouldBe Seq(RawHeader("If-Match", "foobar"))
  }
}
