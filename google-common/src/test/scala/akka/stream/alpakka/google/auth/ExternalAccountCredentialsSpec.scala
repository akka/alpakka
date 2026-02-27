/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.stream.alpakka.google.RequestSettings
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, OptionValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsString, JsonParser}

import java.net.URLDecoder
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class ExternalAccountCredentialsSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with OptionValues
    with ScalaFutures
    with BeforeAndAfter
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  import system.dispatcher
  private implicit val requestSettings: RequestSettings =
    RequestSettings(ConfigFactory.defaultReference().getConfig("alpakka.google"))

  private val StsToken = "some-sts-token"
  private val UrlCredentialSourceToken = "some-url-credential-source-token"
  private val UrlAccessToken = "some-url-access-token"
  private val FileAccessToken = "some-file-access-token"
  private val ImpersonationToken = "some-impersonation-token"

  private def applicationDefaultJson(url: String, sourceFile: Option[String], impersonate: Option[String]): String = {
    s"""
       |{
       |  "type" : "external_account",
       |  "audience" : "some-audience",
       |  "subject_token_type" : "urn:ietf:params:oauth:token-type:jwt",
       |  "token_url" : "$url/sts-token",
       |  "credential_source": {
       |    ${sourceFile match {
         case Some(file) =>
           s"""
             |      "file" : "$file"""".stripMargin
         case None =>
           s"""
             |    "url": "$url/credential-source",
             |    "headers" : {
             |      "Authorization" : "Bearer $UrlCredentialSourceToken"
             |    },
             |    "format" : {
             |      "type" : "json",
             |      "subject_token_field_name" : "value"
             |    }
             |""".stripMargin
       }}
       |  }${impersonate.map(account => s""",
           |  "service_account_impersonation_url" : "$url/impersonate/$account"""").getOrElse("")}
       |}
       |""".stripMargin
  }

  private val impersonationTokenExpireTime = Instant.now().plusSeconds(3600)

  private case class CapturedRequest(request: HttpRequest, entity: HttpEntity.Strict)

  private case class ServerCalls(baseUrl: String,
                                 credentialSource: Future[CapturedRequest],
                                 stsToken: Future[CapturedRequest],
                                 impersonate: Future[CapturedRequest])

  private var bindings: Seq[Http.ServerBinding] = Nil

  after {
    bindings.foreach(_.unbind())
    bindings = Nil
  }

  private def mockServer(): ServerCalls = {
    val credentialSource = Promise[CapturedRequest]()
    val stsToken = Promise[CapturedRequest]()
    val impersonate = Promise[CapturedRequest]()

    val binding = Http()
      .newServerAt("127.0.0.1", 0)
      .bind { request =>
        request.entity.toStrict(1.second).map { entity =>
          val capturedRequest = CapturedRequest(request, entity)
          request.uri.path.toString match {
            case "/credential-source" =>
              credentialSource.success(capturedRequest)
              HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, s"""
               |{
               |  "value": "$UrlAccessToken"
               |}
               |""".stripMargin))
            case "/sts-token" =>
              // Implements https://docs.cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token
              stsToken.success(capturedRequest)
              HttpResponse(
                entity = HttpEntity(
                  ContentTypes.`application/json`,
                  s"""
               |{
               |  "access_token": "$StsToken",
               |  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
               |  "token_type": "Bearer",
               |  "expires_in": 3600
               |}
               |""".stripMargin
                )
              )
            case imp if imp.startsWith("/impersonate/") =>
              // Implements https://docs.cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
              impersonate.success(capturedRequest)
              HttpResponse(
                entity = HttpEntity(
                  ContentTypes.`application/json`,
                  s"""
                |{
                |  "accessToken": "$ImpersonationToken",
                |  "expireTime": "$impersonationTokenExpireTime"
                |}
                |""".stripMargin
                )
              )
          }
        }
      }
      .futureValue
    bindings = bindings :+ binding
    ServerCalls(s"http://127.0.0.1:${binding.localAddress.getPort}",
                credentialSource.future,
                stsToken.future,
                impersonate.future)
  }

  private def mockFile(contents: String): Path = {
    val file = Files.createTempFile("alpakka-google-external-creds-mock-file", "txt")
    Files.write(file, contents.getBytes)
    file.toFile.deleteOnExit()
    file
  }

  private def verifyStsRequest(request: CapturedRequest, subjectToken: String): Unit = {
    // Verify it matches https://datatracker.ietf.org/doc/html/rfc8693, with parameters as described in
    // https://docs.cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token
    val stsParams = request.entity.data.utf8String
      .split("&")
      .map(
        v =>
          v.split("=", 2) match {
            case Array(key, value) => (key, URLDecoder.decode(value, "utf-8"))
            case Array(key) => (key, "")
            case _ => ("", "")
          }
      )
      .toMap
    stsParams.get("grant_type").value should ===("urn:ietf:params:oauth:grant-type:token-exchange")
    stsParams.get("audience").value should ===("some-audience")
    stsParams.get("requested_token_type").value should ===("urn:ietf:params:oauth:token-type:access_token")
    stsParams.get("subject_token_type").value should ===("urn:ietf:params:oauth:token-type:jwt")
    stsParams.get("subject_token").value should ===(subjectToken)
  }

  private def verifyImpersonationRequest(request: CapturedRequest): Unit = {
    // Verify it matches https://docs.cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
    val json = JsonParser(request.entity.data.utf8String).asJsObject
    json.fields.get("lifetime").value should ===(JsString("3600s"))
    request.request.header[Authorization].value.credentials.token() should ===(StsToken)
  }

  private def verifyCredentialSourceRequest(request: CapturedRequest): Unit = {
    request.request.header[Authorization].value.credentials.token() should ===(UrlCredentialSourceToken)
  }

  "The Google External Account credentials support" should {
    "support fetching external credentials using a file" in {
      val file = mockFile(FileAccessToken)
      val server = mockServer()
      val credsJson = applicationDefaultJson(server.baseUrl, Some(file.toString), None)
      val creds =
        ApplicationDefaultCredentials.loadFromJson(credsJson, Seq("https://www.googleapis.com/auth/cloud-platform"))

      val httpCreds = creds.get().futureValue

      httpCreds.token() should ===(StsToken)
      verifyStsRequest(server.stsToken.futureValue, FileAccessToken)
      server.credentialSource.value shouldBe empty
      server.impersonate.value shouldBe empty
    }

    "support fetching external credentials using a file with impersonation support" in {
      val file = mockFile(FileAccessToken)
      val server = mockServer()
      val credsJson = applicationDefaultJson(server.baseUrl, Some(file.toString), Some("service-account"))
      val creds =
        ApplicationDefaultCredentials.loadFromJson(credsJson, Seq("https://www.googleapis.com/auth/cloud-platform"))

      val httpCreds = creds.get().futureValue

      httpCreds.token() should ===(ImpersonationToken)
      verifyStsRequest(server.stsToken.futureValue, FileAccessToken)
      verifyImpersonationRequest(server.impersonate.futureValue)
      server.credentialSource.value shouldBe empty
    }

    "support fetching external credentials using a url" in {
      val server = mockServer()
      val credsJson = applicationDefaultJson(server.baseUrl, None, None)
      val creds =
        ApplicationDefaultCredentials.loadFromJson(credsJson, Seq("https://www.googleapis.com/auth/cloud-platform"))

      val httpCreds = creds.get().futureValue

      httpCreds.token() should ===(StsToken)
      verifyCredentialSourceRequest(server.credentialSource.futureValue)
      verifyStsRequest(server.stsToken.futureValue, UrlAccessToken)
      server.impersonate.value shouldBe empty
    }

    "support fetching external credentials using a url with impersonation support" in {
      val server = mockServer()
      val credsJson = applicationDefaultJson(server.baseUrl, None, Some("service-account"))
      val creds =
        ApplicationDefaultCredentials.loadFromJson(credsJson, Seq("https://www.googleapis.com/auth/cloud-platform"))

      val httpCreds = creds.get().futureValue

      httpCreds.token() should ===(ImpersonationToken)
      verifyCredentialSourceRequest(server.credentialSource.futureValue)
      verifyStsRequest(server.stsToken.futureValue, UrlAccessToken)
      verifyImpersonationRequest(server.impersonate.futureValue)
    }

  }

}
