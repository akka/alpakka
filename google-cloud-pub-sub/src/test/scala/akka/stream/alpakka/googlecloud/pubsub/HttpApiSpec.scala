/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, any, urlEqualTo}
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

class HttpApiSpec extends FlatSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  val wiremockServer = new WireMockServer(
    wireMockConfig().dynamicPort().notifier(new ConsoleNotifier(false))
  )
  wiremockServer.start()

  val mock = new WireMock("localhost", wiremockServer.port())

  private object TestHttpApi extends HttpApi {
    val PubSubGoogleApisHost = s"http://localhost:${wiremockServer.port()}"
    val GoogleApisHost = s"http://localhost:${wiremockServer.port()}"
  }

  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"

  it should "request a auth token" in {
    val expectedRequest =
      "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.CnsKICJpc3MiOiAidGVzdC1YWFhAdGVzdC1YWFhYWC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAic2NvcGUiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vYXV0aC9wdWJzdWIiLAogImF1ZCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjQvdG9rZW4iLAogImV4cCI6IDM2MDAsCiAiaWF0IjogMAp9CiAgICAgIA==.PG_qpgReiRycZftM4evJAiIETNO2yjqL6O0VjtsGaOtrqv8mIl2fI7kSizBxp4f9AY-WIBpAh60T2JpHlUGui0-TNklSH-g3RRGiPk348hi01crSThnk3WjV2WB4F_nfunTWiPR96zVkwPUweBk-Lj151eOHkNEhAHnngvsRAZpVfOiKKi9XA-tPfLCM_VF_e9o7WBrswTA-a-RjI-WZu-S_cJVd2xaxFo1CbccA7n7yzI-3eshaJuqoloSe-u_JAlLo66CdhCRViN06XpveTtwuej4xG-H6BLvNNkF8XxTU2FnbBwLITG4bg_K3T4lfQphpxBqD7Ic7_ciFRKQDAw=="
    val authResult =
      s"""{
        | "access_token": "$accessToken",
        | "token_type": "bearer",
        | "expires_in": 3600
        |}""".stripMargin

    mock.register(
      any(urlEqualTo("/oauth2/v4/token"))
        .withRequestBody(WireMock.equalTo(expectedRequest))
        .willReturn(aResponse().withStatus(200).withBody(authResult).withHeader("Content-Type", "application/json"))
    )

    val result =
      TestHttpApi.getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(0))

    result.futureValue shouldBe AccessTokenExpiry(accessToken, 3600)
  }

  it should "publish" in {

    val publishMessage =
      PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1"}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?key=${TestCredentials.apiKey}")
        )
        .withRequestBody(WireMock.equalTo(expectedPublishRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(publishResponse)
            .withHeader("Content-Type", "application/json")
        )
    )

    val result =
      TestHttpApi.publish(TestCredentials.projectId, "topic1", accessToken, TestCredentials.apiKey, publishRequest)

    result.futureValue shouldBe Seq("1")
  }

  it should "Pull with results" in {

    val publishMessage =
      PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))

    val pullResponse =
      """{"receivedMessages":[{"ackId":"ack1","message":{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1"}}]}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?key=${TestCredentials.apiKey}"
          )
        )
        .withRequestBody(WireMock.equalTo(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val result = TestHttpApi.pull(TestCredentials.projectId, "sub1", accessToken, TestCredentials.apiKey)
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", publishMessage))))

  }

  it should "Pull without results" in {

    val pullResponse = "{}"

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?key=${TestCredentials.apiKey}"
          )
        )
        .withRequestBody(WireMock.equalTo(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val result = TestHttpApi.pull(TestCredentials.projectId, "sub1", accessToken, TestCredentials.apiKey)
    result.futureValue shouldBe PullResponse(None)

  }

  it should "acknowledge" in {
    val ackRequest = """{"ackIds":["ack1"]}"""
    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:acknowledge?key=${TestCredentials.apiKey}"
          )
        )
        .withRequestBody(WireMock.equalTo(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200))
    )

    val acknowledgeRequest = AcknowledgeRequest(Seq("ack1"))

    val result = TestHttpApi.acknowledge(TestCredentials.projectId,
                                         "sub1",
                                         accessToken,
                                         TestCredentials.apiKey,
                                         acknowledgeRequest)

    result.futureValue shouldBe (())
  }

  private val httpApi = HttpApi
  if (httpApi.PubSubEmulatorHost.isDefined) it should "honor emulator host variables" in {
    val emulatorVar = sys.props
      .get(httpApi.PubSubEmulatorHostVarName)
      .orElse(sys.env.get(httpApi.PubSubEmulatorHostVarName))

    emulatorVar.foreach { emulatorHost =>
      httpApi.PubSubGoogleApisHost shouldEqual s"http://$emulatorHost"
      httpApi.GoogleApisHost shouldEqual s"http://$emulatorHost"
    }
  }

  override def afterAll(): Unit = {
    wiremockServer.stop()
    Await.result(system.terminate(), 5.seconds)
  }
}

object TestCredentials {
  val privateKey = {
    val pk =
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxwdLoCIviW0BsREeKziqiSgzl17Q6nD4RhqbB71oPGG8h82EJPeIlLQsMGEtuig0MVsUa9MudewFuQ/XHWtxnueQ3I900EJmrDTA4ysgHcVvyDBPuYdVVV7LE/9nysuHb2x3bh057Sy60qZqDS2hV9ybOBp2RIEK04k/hQDDqp+LxcnNQBi5C0f6aohTN6Ced2vvTY6hWbgFDk4Hdw9JDJpf8TSx/ZxJxPd3EA58SgXRBuamVZWy1IVpFOSKUCr4wwMOrELu9mRGzmNJiLSqn1jqJlG97ogth3dEldSOtwlfVI1M4sDe3k1SnF1+IagfK7Wda5hUidPLLfKn5lzMlAgMBAAECggEAQwjwS5blgfCdw/af8EW9qEQ6xvbove2sLpnUC3EDSowRZQFOh9ixjwmEkAQddktTjmKupHLK0tHPgVDZwFuQoQFmgjDhO8BC41Hu7Iv0kXH7lbVeUtjMRgnznf1KqQ8yw+HHScGmJDL/IxyO65Klfz3cgXfXNiKvZV2veCfoCqWyrI7aaOPiC/6NwKXDxy+Bot44hkJFruOHh67Gunr4TlMcp+z2mQvvCsX6f/bPT0sxamvYY3QyrOMAQO6G0xele1J0lALvxBI5qaE5gysMEEyzELeEayZI4xKlJFZPryyaXl2gfQ1qLFPcZGg+1Qc1Gxxqp3rKazqebj0AiM18xQKBgQDaXLNKN59ChnyCuJqHIOe/+BpAUVXbO9GhtderQTQJR+Jh0NxRWliGIyiKWjazp/+HbAV9YiYuwFUUiBlnz6bAld83Jrl6ad1Q8hAhIbid73/DryGN1CCioKp0C37SoaKewq7jG7N9hIgd8Y/vfNfTfW4+mrwbRmKOwI/wb6xtOwKBgQDQZWpJZ6NpplkMum+AspeNRkYGy8OeAQt9jGeczOqTbVs84Z9rqR3rJYqjfgGBNquj4F6zIb8Emkgx70+mCMJospIVLTlajIbgsQOURrIFwtKU1e01gaZYgm99eHmYPmjQdgGkDU5bZ6bzS8GN7OkotW9FtoFw0nA9ERKrNIZbHwKBgFkr3/+f5UaaewA6+MfT9S/c1oOLc38612mtQ6xozSI5G8aML1x9g4cLloOhQZNuOJiJ0VgZm7Qd0OC4j39oOhWNXoE8LCREVR+4KkQNbEH6yvcTbqVnigg/ijwncZv8a9dfc2HFLzBDzf5EZl4LYmu4XivsroKI5LidBGrQf95/AoGBAMbmtN5g25hf3AiI/RmR25JMa1PbMdbh2my3EMGY159ktbtTAUzJejPQfhVzk84XNxVPdjN01xN2iceXSKcJHzy8iy9JHb+t9qIIcYkZPJrBCyphUGlMWE+MFwtjbHMBxhqJNyG0TYByWudF+/QRFaz0FsMr4TmksNmoLPBZTo8zAoGBAKZIvf5XBlTqd/tR4cnTBQOeeegTHT5x7e+W0mfpCo/gDDmKnOsF2lAwj/F/hM5WqorHoM0ibno+0zUb5q6rhccAm511h0LmV1taVkbWk4UReuPuN+UyVUP+IjmXjagDle9IkOE7+fDlNb+Q7BHl2R8zm1jZjEDwM2NQnSxQ22+/"
    val kf = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode(pk)
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }
  val clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com"
  val projectId = "testX-XXXXX"
  val apiKey = "AIzaSyCVvqrlz057gCssc70n5JERyTW4TpB4ebE"
}
