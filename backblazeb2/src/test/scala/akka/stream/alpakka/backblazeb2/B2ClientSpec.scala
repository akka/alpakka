package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import akka.util.ByteString
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.Matchers._
import JsonSupport._
import akka.http.scaladsl.model.StatusCodes
import io.circe.syntax._

class B2ClientSpec extends WireMockBase {
  val accountId = AccountId("accountId")
  val applicationKey = ApplicationKey("applicationKey")
  val credentials = B2AccountCredentials(accountId, applicationKey)

  implicit val system = ActorSystem("B2ClientSpec-system", config)
  implicit val materializer = ActorMaterializer()

  val bucketId = BucketId("testBucketId")
  val hostAndPort = s"localhost:${mockServer.httpsPort}"
  val client = new B2Client(credentials, bucketId, hostAndPort)

  val fileName = FileName("test")
  val data = ByteString("test")

  private def successfulJsonResponse(returnData: String) = {
    aResponse()
      .withStatus(StatusCodes.OK.intValue)
      .withHeader("Content-Type", "application/json; charset=UTF-8")
      .withBody(returnData)
  }

  private def mockGet(url: String, returnData: String) = {
    mock.register(
      get(urlEqualTo(url))
        .willReturn(successfulJsonResponse(returnData))
      )
  }

  private def mockPost(url: String, returnData: String) = {
    mock.register(
      post(urlEqualTo(url))
        .willReturn(successfulJsonResponse(returnData))
    )
  }

  val successfulAuthorizeAccountResponse = AuthorizeAccountResponse(accountId, ApiUrl(s"https://$hostAndPort"), AccountAuthorizationToken("accountAuthorizationToken"))
  val successfulAuthorizeAccountResponseJson = successfulAuthorizeAccountResponse.asJson.noSpaces
  val successfulUploadUrl = UploadUrl(s"https://$hostAndPort/successfulUploadUrl")
  val successfulUploadAuthorizationToken = UploadAuthorizationToken("successfulUploadAuthorizationToken")
  val successfulGetUploadUrlResponse = GetUploadUrlResponse(bucketId, successfulUploadUrl, successfulUploadAuthorizationToken)
  val successfulGetUploadUrlResponseJson = successfulGetUploadUrlResponse.asJson.noSpaces
  val successfulUploadFileResponse = UploadFileResponse(
    fileId = FileId("fileId"),
    fileName = fileName,
    accountId = accountId,
    bucketId = bucketId,
    contentLength = 0,
    contentSha1 = Sha1("sha1"),
    contentType = "application/text",
    fileInfo = Map.empty
  )
  val successfulUploadFileResponseJson = successfulUploadFileResponse.asJson.noSpaces

  it should "work for happy case" in {
    mockGet("/b2api/v1/b2_authorize_account", successfulAuthorizeAccountResponseJson)
    mockGet(s"/b2api/v1/b2_get_upload_url?bucketId=$bucketId", successfulGetUploadUrlResponseJson)
    mockPost("/successfulUploadUrl", successfulUploadFileResponseJson)

    val resultF = client.upload(fileName, data)
    val result = extractFromResponse(resultF)
    result shouldBe a[UploadFileResponse]
  }
}
