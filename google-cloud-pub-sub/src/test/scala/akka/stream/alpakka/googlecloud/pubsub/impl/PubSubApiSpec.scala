/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub._
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, urlEqualTo}
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

class PubSubApiSpec extends FlatSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  val wiremockServer = new WireMockServer(
    wireMockConfig().dynamicPort().notifier(new ConsoleNotifier(false))
  )
  wiremockServer.start()

  val mock = new WireMock("localhost", wiremockServer.port())

  private object TestHttpApi extends PubSubApi {
    val isEmulated = false
    val PubSubGoogleApisHost = s"http://localhost:${wiremockServer.port()}"
    val GoogleApisHost = s"http://localhost:${wiremockServer.port()}"
  }

  val http: HttpExt = null
  val config = new PubSubConfig(TestCredentials.projectId,
                                TestCredentials.apiKey,
                                TestCredentials.clientEmail,
                                TestCredentials.privateKey,
                                http)

  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"

  it should "publish" in {

    val publishMessage =
      PubSubMessage(
        messageId = "1",
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)),
        attributes = Some(Map("row_id" -> "7")),
        publishTime = Some(Instant.parse("2014-10-02T15:01:23.045123456Z"))
      )
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1","attributes":{"row_id":"7"},"publishTime":"2014-10-02T15:01:23.045123456Z"}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
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
      TestHttpApi.publish(config.projectId, "topic1", Some(accessToken), config.apiKey, publishRequest)

    result.futureValue shouldBe Seq("1")
  }

  it should "publish without Authorization header to emulator" in {

    object TestEmulatorHttpApi extends PubSubApi {
      override val isEmulated = true
      val PubSubGoogleApisHost = s"http://localhost:${wiremockServer.port()}"
      val GoogleApisHost = s"http://localhost:${wiremockServer.port()}"
    }

    val publishMessage =
      PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1"}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
        )
        .withRequestBody(WireMock.equalTo(expectedPublishRequest))
        .withHeader("Authorization", WireMock.absent())
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(publishResponse)
            .withHeader("Content-Type", "application/json")
        )
    )

    val result =
      TestEmulatorHttpApi.publish(config.projectId, "topic1", None, config.apiKey, publishRequest)

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
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalTo(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val result = TestHttpApi.pull(config.projectId, "sub1", Some(accessToken), config.apiKey)
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", publishMessage))))

  }

  it should "Pull with results without access token in emulated mode" in {
    object TestEmulatorHttpApi extends PubSubApi {
      override val isEmulated = true
      val PubSubGoogleApisHost = s"http://localhost:${wiremockServer.port()}"
      val GoogleApisHost = s"http://localhost:${wiremockServer.port()}"
    }

    val publishMessage =
      PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))

    val pullResponse =
      """{"receivedMessages":[{"ackId":"ack1","message":{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1"}}]}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalTo(pullRequest))
        .withHeader("Authorization", WireMock.absent())
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val result = TestEmulatorHttpApi.pull(config.projectId, "sub1", None, config.apiKey)
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", publishMessage))))

  }

  it should "Pull without results" in {

    val pullResponse = "{}"

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalTo(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val result = TestHttpApi.pull(config.projectId, "sub1", Some(accessToken), config.apiKey)
    result.futureValue shouldBe PullResponse(None)

  }

  it should "acknowledge" in {
    val ackRequest = """{"ackIds":["ack1"]}"""
    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:acknowledge"
          )
        )
        .withRequestBody(WireMock.equalTo(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200))
    )

    val acknowledgeRequest = AcknowledgeRequest(Seq("ack1"))

    val result = TestHttpApi.acknowledge(config.projectId, "sub1", Some(accessToken), config.apiKey, acknowledgeRequest)

    result.futureValue shouldBe (())
  }

  it should "return exception with the meaningful error message in case of not successful publish response" in {
    val publishMessage =
      PubSubMessage(
        messageId = "1",
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)),
        attributes = Some(Map("row_id" -> "7")),
        publishTime = Some(Instant.parse("2014-10-02T15:01:23.045123456Z"))
      )

    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1","attributes":{"row_id":"7"},"publishTime":"2014-10-02T15:01:23.045123456Z"}]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
        )
        .withRequestBody(WireMock.equalTo(expectedPublishRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
            .withBody("{}")
            .withHeader("Content-Type", "application/json")
        )
    )

    val result =
      TestHttpApi.publish(config.projectId, "topic1", Some(accessToken), config.apiKey, publishRequest)

    assertThrows[RuntimeException] { result.futureValue }
  }

  private val httpApi = PubSubApi
  if (httpApi.PubSubEmulatorHost.isDefined) it should "honor emulator host variables" in {
    val emulatorVar = sys.props
      .get(httpApi.PubSubEmulatorHostVarName)
      .orElse(sys.env.get(httpApi.PubSubEmulatorHostVarName))

    emulatorVar.foreach { emulatorHost =>
      httpApi.isEmulated shouldBe true
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
  val privateKey = """-----BEGIN RSA PRIVATE KEY-----
                     |MIIBOgIBAAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V84K5dgzhR9TFpkAp2kl2
                     |9BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQJAQVyXbMS7TGDFWnXieKZh
                     |Dm/uYA6sEJqheB4u/wMVshjcQdHbi6Rr0kv7dCLbJz2v9bVmFu5i8aFnJy1MJOpA
                     |2QIhAPyEAaVfDqJGjVfryZDCaxrsREmdKDlmIppFy78/d8DHAiEAk9JyTHcapckD
                     |uSyaE6EaqKKfyRwSfUGO1VJXmPjPDRMCIF9N900SDnTiye/4FxBiwIfdynw6K3dW
                     |fBLb6uVYr/r7AiBUu/p26IMm6y4uNGnxvJSqe+X6AxR6Jl043OWHs4AEbwIhANuz
                     |Ay3MKOeoVbx0L+ruVRY5fkW+oLHbMGtQ9dZq7Dp9
                     |-----END RSA PRIVATE KEY-----""".stripMargin
  val clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com"
  val projectId = "testX-XXXXX"
  val apiKey = "AIzaSyCVvqrlz057gCssc70n5JERyTW4TpB4ebE"
}
