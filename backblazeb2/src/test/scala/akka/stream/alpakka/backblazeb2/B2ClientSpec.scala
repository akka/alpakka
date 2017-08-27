/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import akka.util.ByteString
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.Matchers._
import SerializationSupport._
import akka.http.scaladsl.model.StatusCodes
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.circe.syntax._
import org.scalatest.{Assertion, BeforeAndAfterEach}

class B2ClientSpec extends WireMockBase with BeforeAndAfterEach {
  val accountId = AccountId("accountId")
  val applicationKey = ApplicationKey("applicationKey")
  val credentials = B2AccountCredentials(accountId, applicationKey)

  implicit val system = ActorSystem("B2ClientSpec-system", config)
  implicit val materializer = ActorMaterializer()

  val bucketName = BucketName("testBucketName")
  val bucketId = BucketId("testBucketId")
  val hostAndPort = s"localhost:${mockServer.httpsPort}"

  def createClient() = new B2Client(credentials, bucketId, eagerAuthorization = false, hostAndPort)

  val fileName = FileName("testFileName")
  val data = ByteString("testData")

  private def jsonResponse(returnData: String, status: Int = StatusCodes.OK.intValue) =
    aResponse().withStatus(status).withHeader("Content-Type", "application/json; charset=UTF-8").withBody(returnData)

  private def mockGet(url: String, returnData: String, status: Int = StatusCodes.OK.intValue) =
    mock.register(
      get(urlEqualTo(url)).willReturn(jsonResponse(returnData, status))
    )

  val validAccountAuthorizationToken = "validAccountAuthorizationToken"
  val successfulAuthorizeAccountResponse = AuthorizeAccountResponse(
    accountId,
    ApiUrl(s"https://$hostAndPort"),
    AccountAuthorizationToken(validAccountAuthorizationToken)
  )
  val successfulAuthorizeAccountResponseJson = successfulAuthorizeAccountResponse.asJson.noSpaces

  val expiredAccountAuthorizationToken = "expiredAccountAuthorizationToken"
  val expiredAuthorizeAccountResponse = AuthorizeAccountResponse(
    accountId,
    ApiUrl(s"https://$hostAndPort"),
    AccountAuthorizationToken(expiredAccountAuthorizationToken)
  )
  val expiredAuthorizeAccountResponseJson = expiredAuthorizeAccountResponse.asJson.noSpaces

  val expiredTokenResponse =
    B2ErrorResponse(ErrorCodes.ExpiredAuthToken, "expired_auth_token", "Authorization token has expired")
  val expiredTokenResponseJson = expiredTokenResponse.asJson.noSpaces

  val successfulUploadUrlPath = "/successfulUploadUrl"
  val successfulUploadUrl = UploadUrl(s"https://$hostAndPort$successfulUploadUrlPath")
  val validUploadAuthorizationToken = "validUploadAuthorizationToken"

  val expiredUploadUrlPath = "/expiredUploadUrl"
  val expiredUploadUrl = UploadUrl(s"https://$hostAndPort$expiredUploadUrlPath")
  val expiredUploadAuthorizationToken = "expiredUploadAuthorizationToken"

  val successfulGetUploadUrlResponse =
    GetUploadUrlResponse(bucketId, successfulUploadUrl, UploadAuthorizationToken(validUploadAuthorizationToken))
  val successfulGetUploadUrlResponseJson = successfulGetUploadUrlResponse.asJson.noSpaces

  val expiredGetUploadUrlResponse =
    GetUploadUrlResponse(bucketId, expiredUploadUrl, UploadAuthorizationToken(expiredUploadAuthorizationToken))
  val expiredGetUploadUrlResponseJson = expiredGetUploadUrlResponse.asJson.noSpaces

  val fileId = FileId("fileId")

  val successfulUploadFileResponse = UploadFileResponse(
    fileId = fileId,
    fileName = fileName,
    accountId = accountId,
    bucketId = bucketId,
    contentLength = 0,
    contentSha1 = Sha1("sha1"),
    contentType = "application/text",
    fileInfo = Map.empty
  )
  val successfulUploadFileResponseJson = successfulUploadFileResponse.asJson.noSpaces

  val getUploadUrlPath = s"/b2api/v1/b2_get_upload_url?bucketId=$bucketId"
  val AuthorizationHeader = "Authorization"

  private def mockSuccessfulAuthorizeAccount() =
    mockGet("/b2api/v1/b2_authorize_account", successfulAuthorizeAccountResponseJson)

  private val expiredTokenJson = jsonResponse(expiredTokenResponseJson, ErrorCodes.ExpiredAuthToken)

  private def mockUploadPaths() = {
    mock.register(
      post(urlEqualTo(expiredUploadUrlPath))
        .withHeader(AuthorizationHeader, equalTo(expiredUploadAuthorizationToken))
        .willReturn(expiredTokenJson)
    )

    mock.register(
      post(urlEqualTo(successfulUploadUrlPath))
        .withHeader(AuthorizationHeader, equalTo(validUploadAuthorizationToken))
        .willReturn(jsonResponse(successfulUploadFileResponseJson))
    )
  }

  private def mockGetUploadUrl() = {
    mock.register(
      get(urlEqualTo(getUploadUrlPath))
        .withHeader(AuthorizationHeader, equalTo(validAccountAuthorizationToken))
        .willReturn(jsonResponse(successfulGetUploadUrlResponseJson))
    )

    mock.register(
      get(urlEqualTo(getUploadUrlPath))
        .withHeader(AuthorizationHeader, equalTo(expiredAccountAuthorizationToken))
        .willReturn(expiredTokenJson)
    )
  }

  private def uploadTest(client: B2Client): Assertion = {
    val resultF = client.upload(fileName, data)
    val result = extractFromResponse(resultF)
    result shouldBe a[UploadFileResponse]
  }

  it should "work for happy case" in {
    val client = createClient()

    mockSuccessfulAuthorizeAccount()
    mockGetUploadUrl()
    mockUploadPaths()

    uploadTest(client)
  }

  val expiredAlreadyReturned = "expiredAlreadyReturned"

  /**
   * Returns an expired authorize account response upon the first invocation, then a successful one on consequent ones
   */
  private def mockAuthorizeAccountFirstExpiredThenValid(scenario: String = "Authorize Account") = {
    val url = "/b2api/v1/b2_authorize_account"

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

  it should "handle expired account authorization token" in {
    val client = createClient()

    mockAuthorizeAccountFirstExpiredThenValid()
    mockGetUploadUrl()
    mockUploadPaths()

    uploadTest(client)
  }

  /**
   * Returns an expired "get upload URL" response upon the first invocation, then a successful one on consequent ones
   */
  private def mockUploadUrlFirstExpiredThenValid(scenario: String = "Get upload URL") = {
    mock.register(
      get(urlEqualTo(getUploadUrlPath))
        .inScenario(scenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .withHeader(AuthorizationHeader, equalTo(validAccountAuthorizationToken))
        .willReturn(jsonResponse(expiredGetUploadUrlResponseJson))
        .willSetStateTo(expiredAlreadyReturned)
    )

    mock.register(
      get(urlEqualTo(getUploadUrlPath))
        .inScenario(scenario)
        .whenScenarioStateIs(expiredAlreadyReturned)
        .withHeader(AuthorizationHeader, equalTo(validAccountAuthorizationToken))
        .willReturn(jsonResponse(successfulGetUploadUrlResponseJson))
    )
  }

  it should "handle expired upload authorization token" in {
    val client = createClient()

    mockSuccessfulAuthorizeAccount()
    mockUploadUrlFirstExpiredThenValid()
    mockUploadPaths()

    uploadTest(client)
  }

  private def checkDownloadFileResponse(downloadF: B2Response[DownloadFileResponse]): Assertion = {
    val download = extractFromResponse[DownloadFileResponse](downloadF)
    download.fileId shouldEqual fileId
    download.data shouldEqual data
  }

  private def downloadByIdTest(client: B2Client): Assertion = {
    val downloadF = client.downloadById(fileId)
    checkDownloadFileResponse(downloadF)
  }

  private def downloadByNameTest(client: B2Client): Assertion = {
    val downloadF = client.downloadByName(fileName, bucketName)
    checkDownloadFileResponse(downloadF)
  }

  private def mockDownloadFileSetup(url: String) = {
    mock.register(
      get(urlEqualTo(url))
        .withHeader(AuthorizationHeader, equalTo(validAccountAuthorizationToken))
        .willReturn(
          aResponse()
            .withStatus(StatusCodes.OK.intValue)
            .withHeader("Content-Type", "application/json; charset=UTF-8")
            .withHeader("X-Bz-File-Id".toLowerCase, fileId.value)
            .withHeader("X-Bz-File-Name".toLowerCase, fileName.value)
            .withHeader("X-Bz-Content-Sha1".toLowerCase, "sha1")
            .withBody(data.toArray)
        )
    )

    mock.register(
      get(urlEqualTo(url))
        .withHeader(AuthorizationHeader, equalTo(expiredAccountAuthorizationToken))
        .willReturn(expiredTokenJson)
    )
  }

  it should "handle download by id" in {
    val client = createClient()

    mockAuthorizeAccountFirstExpiredThenValid()

    val downloadUrl = s"/b2api/v1/b2_download_file_by_id?fileId=$fileId"
    mockDownloadFileSetup(downloadUrl)

    downloadByIdTest(client)
  }

  it should "handle download by name" in {
    val client = createClient()

    mockAuthorizeAccountFirstExpiredThenValid()
    val downloadUrl = s"/file/$bucketName/$fileName"
    mockDownloadFileSetup(downloadUrl)

    downloadByNameTest(client)
  }

  override protected def afterEach(): Unit = {
    mock.resetScenarios()
    mock.resetMappings()
    mock.resetRequests()
  }
}
