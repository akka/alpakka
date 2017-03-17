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
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.circe.syntax._

class B2ClientSpec extends WireMockBase {
  val accountId = AccountId("accountId")
  val applicationKey = ApplicationKey("applicationKey")
  val credentials = B2AccountCredentials(accountId, applicationKey)

  implicit val system = ActorSystem("B2ClientSpec-system", config)
  implicit val materializer = ActorMaterializer()

  val bucketId = BucketId("testBucketId")
  val hostAndPort = s"localhost:${mockServer.httpsPort}"

  def createClient() = new B2Client(credentials, bucketId, hostAndPort)

  val fileName = FileName("test")
  val data = ByteString("test")

  private def jsonResponse(returnData: String, status: Int = StatusCodes.OK.intValue) = {
    aResponse()
      .withStatus(status)
      .withHeader("Content-Type", "application/json; charset=UTF-8")
      .withBody(returnData)
  }

  private def mockGet(url: String, returnData: String) = {
    mock.register(
      get(urlEqualTo(url))
        .willReturn(jsonResponse(returnData))
      )
  }

  private def mockPost(url: String, returnData: String) = {
    mock.register(
      post(urlEqualTo(url))
        .willReturn(jsonResponse(returnData))
    )
  }

  val validAccountAuthorizationToken = "validAccountAuthorizationToken"
  val successfulAuthorizeAccountResponse = AuthorizeAccountResponse(accountId, ApiUrl(s"https://$hostAndPort"), AccountAuthorizationToken(validAccountAuthorizationToken))
  val successfulAuthorizeAccountResponseJson = successfulAuthorizeAccountResponse.asJson.noSpaces

  val expiredAccountAuthorizationToken = "expiredAccountAuthorizationToken"
  val expiredAuthorizeAccountResponse = AuthorizeAccountResponse(accountId, ApiUrl(s"https://$hostAndPort"), AccountAuthorizationToken(expiredAccountAuthorizationToken))
  val expiredAuthorizeAccountResponseJson = expiredAuthorizeAccountResponse.asJson.noSpaces

  val ExpiredAuthToken = 401
  val expiredTokenResponse = B2ErrorResponse(ExpiredAuthToken, "expired_auth_token", "Authorization token has expired")
  val expiredTokenResponseJson = expiredTokenResponse.asJson.noSpaces

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

  private def mockGetUploadUrl() = {
    val url = s"/b2api/v1/b2_get_upload_url?bucketId=$bucketId"

    mock.register(
      get(urlEqualTo(url))
        .withHeader("Authorization", equalTo(validAccountAuthorizationToken))
        .willReturn(jsonResponse(successfulGetUploadUrlResponseJson))
    )

    mock.register(
      get(urlEqualTo(url))
        .withHeader("Authorization", equalTo(expiredAccountAuthorizationToken))
        .willReturn(jsonResponse(expiredTokenResponseJson, ExpiredAuthToken))
    )
  }

  private def uploadTest(client: B2Client) = {
    val resultF = client.upload(fileName, data)
    val result = extractFromResponse(resultF)
    result shouldBe a[UploadFileResponse]
  }

  it should "work for happy case" in {
    val client = createClient()

    mockGet("/b2api/v1/b2_authorize_account", successfulAuthorizeAccountResponseJson)
    mockGetUploadUrl()

    mockPost("/successfulUploadUrl", successfulUploadFileResponseJson)

    uploadTest(client)
  }

  /**
    * Returns an expired authorize account response upon the first invocation, then a successful one on consequent ones
    */
  private def mockAuthorizeAccountFirstExpiredThenValid() = {
    val scenario = "Authorize Account"
    val url = "/b2api/v1/b2_authorize_account"
    val expiredAlreadyReturned = "expiredAlreadyReturned"

    mock.register(
      get(urlEqualTo(url))
        .inScenario(scenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(jsonResponse(expiredAuthorizeAccountResponseJson))
        .willSetStateTo(expiredAlreadyReturned)
    )

    mock.register(
      get(urlEqualTo(url))
        .inScenario(scenario)
        .whenScenarioStateIs(expiredAlreadyReturned)
        .willReturn(jsonResponse(successfulAuthorizeAccountResponseJson))
    )
  }

  it should "handle expired account authorization token" in pendingUntilFixed {
    val client = createClient()

    mockAuthorizeAccountFirstExpiredThenValid()
    mockGetUploadUrl()
    mockPost("/successfulUploadUrl", successfulUploadFileResponseJson)

    uploadTest(client)
  }
}
