/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package impl.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpMethods}
import akka.http.scaladsl.model.headers.{
  `Content-Length`,
  `Content-Type`,
  `If-Match`,
  `If-Modified-Since`,
  ByteRange,
  Range
}
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption
import akka.stream.alpakka.azure.storage.requests.{GetBlob, GetFile, PutBlockBlob}
import akka.testkit.TestKit
import com.azure.storage.common.StorageSharedKeyCredential
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URL
import java.time.{Clock, Instant, ZoneOffset}
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.jdk.CollectionConverters._

class SignerSpec
    extends TestKit(ActorSystem("SignerSystem"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private implicit val clock: Clock = Clock.fixed(Instant.parse("2024-09-02T12:00:00Z"), ZoneOffset.UTC)
  private val objectPath = "my-container/MY-BLOB.csv"
  private val filePath = "my-directory/my-sub-directory/my-file.csv"
  private lazy val storageSettings = StorageExt(system).settings("azurite").withEndPointUrl("")
  private lazy val storageSharedKeyCredential = new StorageSharedKeyCredential(
    storageSettings.azureNameKeyCredential.accountName,
    system.settings.config.getString("azurite.credentials.account-key")
  )

  private val leaseId = "ABC123"
  private val range = ByteRange(0, 50)
  private val versionId = "12345XYZ"
  private val customerKey = "EqqWHbls3Y1Hp5B+IS5oUA=="

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate().futureValue
  }

  it should "sign request with no parameters" in {
    val requestBuilder = GetBlob()
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)

    val signer = Signer(request, storageSettings)
    signer.generateAuthorizationHeader shouldBe generateAuthorizationHeader(BlobType,
                                                                            objectPath,
                                                                            HttpMethods.GET.name(),
                                                                            Map(`Content-Length`.name -> "0"))
  }

  it should "sign request with one optional parameter" in {
    val requestBuilder = GetBlob().withLeaseId(leaseId)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)
    val expectedValue = generateAuthorizationHeader(BlobType,
                                                    objectPath,
                                                    HttpMethods.GET.name(),
                                                    Map(`Content-Length`.name -> "0", LeaseIdHeaderKey -> leaseId))
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with multiple parameters" in {
    val requestBuilder = GetBlob().withLeaseId(leaseId).withRange(range)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)
    val expectedValue =
      generateAuthorizationHeader(
        BlobType,
        objectPath,
        HttpMethods.GET.name(),
        Map(`Content-Length`.name -> "0",
            LeaseIdHeaderKey -> leaseId,
            Range.name -> s"bytes=${range.first}-${range.last}")
      )
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with multiple parameters and query parameter" in {
    val requestBuilder = GetBlob().withLeaseId(leaseId).withRange(range).withVersionId(versionId)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)
    val expectedValue =
      generateAuthorizationHeader(
        BlobType,
        objectPath,
        HttpMethods.GET.name(),
        Map(`Content-Length`.name -> "0",
            LeaseIdHeaderKey -> leaseId,
            Range.name -> s"bytes=${range.first}-${range.last}"),
        Some(s"versionId=$versionId")
      )
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with only query parameter" in {
    val requestBuilder = GetFile().withVersionId(versionId)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = FileType, objectPath = filePath)
    val expectedValue =
      generateAuthorizationHeader(
        BlobType,
        filePath,
        HttpMethods.GET.name(),
        Map(`Content-Length`.name -> "0"),
        Some(s"versionId=$versionId")
      )
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with conditional headers" in {
    val requestBuilder =
      GetFile()
        .addHeader("", "")
        .addHeader(`If-Match`.name, "abXzWj65")
        .addHeader(`If-Modified-Since`.name, getFormattedDate)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = FileType, objectPath = filePath)
    val expectedValue =
      generateAuthorizationHeader(
        BlobType,
        filePath,
        HttpMethods.GET.name(),
        Map(`Content-Length`.name -> "0", `If-Match`.name -> "abXzWj65", `If-Modified-Since`.name -> getFormattedDate)
      )
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with required parameters" in {
    val requestBuilder = PutBlockBlob(1024, ContentTypes.`text/csv(UTF-8)`)
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)
    val expectedValue =
      generateAuthorizationHeader(
        BlobType,
        objectPath,
        HttpMethods.PUT.name(),
        Map(`Content-Length`.name -> "1024",
            `Content-Type`.name -> ContentTypes.`text/csv(UTF-8)`.value,
            "x-ms-blob-type" -> BlockBlobType)
      )
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  it should "sign request with ServerSideEncryption enabled" in {
    val requestBuilder =
      PutBlockBlob(1024, ContentTypes.`text/csv(UTF-8)`)
        .withServerSideEncryption(ServerSideEncryption.customerKey(customerKey))
    val request =
      requestBuilder.createRequest(settings = storageSettings, storageType = BlobType, objectPath = objectPath)
    val expectedValue = {
      generateAuthorizationHeader(
        BlobType,
        objectPath,
        HttpMethods.PUT.name(),
        Map(
          `Content-Length`.name -> "1024",
          `Content-Type`.name -> ContentTypes.`text/csv(UTF-8)`.value,
          "x-ms-blob-type" -> BlockBlobType,
          "x-ms-encryption-algorithm" -> "AES256",
          "x-ms-encryption-key" -> customerKey,
          "x-ms-encryption-key-sha256" -> "Zq+2UiSyaBzexl9Y1S/TzJssWRSwsfTnPsMKA+Kew2g="
        )
      )
    }
    Signer(request, storageSettings).generateAuthorizationHeader shouldBe expectedValue
  }

  // generates authorization header using Azure API
  private def generateAuthorizationHeader(storageType: String,
                                          objectPath: String,
                                          httpMethod: String,
                                          headers: Map[String, String],
                                          maybeQueryString: Option[String] = None) = {
    val queryString = maybeQueryString.map(value => s"?$value").getOrElse("")
    val allHeaders = headers ++ Map("x-ms-date" -> getFormattedDate, "x-ms-version" -> storageSettings.apiVersion)
    val url = new URL(
      s"https://${storageSharedKeyCredential.getAccountName}.$storageType.core.windows.net/$objectPath$queryString"
    )
    storageSharedKeyCredential.generateAuthorizationHeader(url, httpMethod, allHeaders.asJava)
  }

  // -----------------------------------------------------------------------
  // SharedKeyLite tests
  // -----------------------------------------------------------------------

  private lazy val sharedKeyLiteSettings = storageSettings.withAuthorizationType(SharedKeyLiteAuthorizationType)

  it should "sign request with SharedKeyLite (no comp parameter)" in {
    val request =
      GetBlob().createRequest(settings = sharedKeyLiteSettings, storageType = BlobType, objectPath = objectPath)

    val expected = sharedKeyLiteAuthorizationHeader(
      verb = HttpMethods.GET.name(),
      contentMd5 = "",
      contentType = "",
      xmsHeaders = Seq("x-ms-date" -> getFormattedDate, "x-ms-version" -> sharedKeyLiteSettings.apiVersion),
      canonicalizedResource = s"/${sharedKeyLiteSettings.azureNameKeyCredential.accountName}/$objectPath"
    )
    Signer(request, sharedKeyLiteSettings).generateAuthorizationHeader shouldBe expected
  }

  it should "sign request with SharedKeyLite ignoring non-comp query parameters" in {
    val request = GetBlob()
      .withVersionId(versionId)
      .createRequest(settings = sharedKeyLiteSettings, storageType = BlobType, objectPath = objectPath)

    // versionId must NOT influence the SharedKeyLite signature.
    val expected = sharedKeyLiteAuthorizationHeader(
      verb = HttpMethods.GET.name(),
      contentMd5 = "",
      contentType = "",
      xmsHeaders = Seq("x-ms-date" -> getFormattedDate, "x-ms-version" -> sharedKeyLiteSettings.apiVersion),
      canonicalizedResource = s"/${sharedKeyLiteSettings.azureNameKeyCredential.accountName}/$objectPath"
    )
    Signer(request, sharedKeyLiteSettings).generateAuthorizationHeader shouldBe expected
  }

  it should "sign PutBlockBlob request with SharedKeyLite (Content-Type included, Content-Length is not)" in {
    val request = PutBlockBlob(1024, ContentTypes.`text/csv(UTF-8)`)
      .createRequest(settings = sharedKeyLiteSettings, storageType = BlobType, objectPath = objectPath)

    val expected = sharedKeyLiteAuthorizationHeader(
      verb = HttpMethods.PUT.name(),
      contentMd5 = "",
      contentType = ContentTypes.`text/csv(UTF-8)`.value,
      xmsHeaders = Seq(
        "x-ms-blob-type" -> BlockBlobType,
        "x-ms-date" -> getFormattedDate,
        "x-ms-version" -> sharedKeyLiteSettings.apiVersion
      ),
      canonicalizedResource = s"/${sharedKeyLiteSettings.azureNameKeyCredential.accountName}/$objectPath"
    )
    Signer(request, sharedKeyLiteSettings).generateAuthorizationHeader shouldBe expected
  }

  // Independent reference implementation of the SharedKeyLite StringToSign per the Azure
  // spec for Blob/Queue/File services. We use this in tests to verify Signer's output
  // without relying on Azure's SDK (which only exposes SharedKey).
  private def sharedKeyLiteAuthorizationHeader(verb: String,
                                               contentMd5: String,
                                               contentType: String,
                                               xmsHeaders: Seq[(String, String)],
                                               canonicalizedResource: String): String = {
    val canonicalizedHeaders =
      xmsHeaders.sortBy(_._1.toLowerCase).map { case (k, v) => s"${k.toLowerCase}:$v" }
    val stringToSign =
      (Seq(verb.toUpperCase, contentMd5, contentType, "") ++ canonicalizedHeaders ++ Seq(canonicalizedResource))
        .mkString("\n")
    val mac = Mac.getInstance(sharedKeyLiteSettings.algorithm)
    mac.init(
      new SecretKeySpec(sharedKeyLiteSettings.azureNameKeyCredential.accountKey, sharedKeyLiteSettings.algorithm)
    )
    val signature = Base64.getEncoder.encodeToString(mac.doFinal(stringToSign.getBytes))
    s"$SharedKeyLiteAuthorizationType ${sharedKeyLiteSettings.azureNameKeyCredential.accountName}:$signature"
  }

}
