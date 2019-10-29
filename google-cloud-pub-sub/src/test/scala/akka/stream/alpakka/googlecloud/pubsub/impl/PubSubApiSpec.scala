/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import java.security.cert.X509Certificate
import java.time.Instant
import java.util.Base64

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, urlEqualTo}
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import javax.net.ssl.{SSLContext, X509TrustManager}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

class NoopTrustManager extends X509TrustManager {
  override def getAcceptedIssuers = new Array[X509Certificate](0)

  override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    //do nothing
  }

  override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    //do nothing
  }
}

class PubSubApiSpec extends FlatSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  val sslContext: SSLContext = SSLContext.getInstance("TLS")
  sslContext.init(null, Array(new NoopTrustManager()), null)

  Http().setDefaultClientHttpsContext(ConnectionContext.https(sslContext))

  val wiremockServer = new WireMockServer(
    wireMockConfig().dynamicPort().dynamicHttpsPort().notifier(new ConsoleNotifier(false))
  )
  wiremockServer.start()

  val mock = new WireMock("localhost", wiremockServer.port())

  private object TestHttpApi extends PubSubApi {
    val isEmulated = false
    val PubSubGoogleApisHost = "localhost"
    val PubSubGoogleApisPort = wiremockServer.httpsPort()
  }

  private object TestEmulatorHttpApi extends PubSubApi {
    override val isEmulated = true
    val PubSubGoogleApisHost = "localhost"
    val PubSubGoogleApisPort = wiremockServer.port()
  }

  val config = PubSubConfig(TestCredentials.projectId, TestCredentials.clientEmail, TestCredentials.privateKey)

  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"

  it should "publish" in {

    val publishMessage =
      PublishMessage(
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)),
        attributes = Map("row_id" -> "7")
      )
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","attributes":{"row_id":"7"}}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
        )
        .withRequestBody(WireMock.equalToJson(expectedPublishRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(publishResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
    val flow = TestHttpApi.publish[Unit](config.projectId, "topic1", 1)
    val result =
      Source.single((publishRequest, Some(accessToken), ())).via(flow).toMat(Sink.head)(Keep.right).run
    result.futureValue._1 shouldBe Seq("1")
    result.futureValue._2 shouldBe (())
  }

  it should "publish without Authorization header to emulator" in {

    val publishMessage =
      PublishMessage(data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ=="}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
        )
        .withRequestBody(WireMock.equalToJson(expectedPublishRequest))
        .withHeader("Authorization", WireMock.absent())
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(publishResponse)
            .withHeader("Content-Type", "application/json")
        )
    )

    val flow = TestEmulatorHttpApi.publish[Unit](config.projectId, "topic1", 1)
    val result =
      Source.single((publishRequest, None, ())).via(flow).toMat(Sink.last)(Keep.right).run
    result.futureValue._1 shouldBe Seq("1")
    result.futureValue._2 shouldBe (())
  }

  it should "Pull with results" in {

    val ts = Instant.parse("2019-07-04T08:10:00.111Z")

    val message =
      PubSubMessage(messageId = "1",
                    data = Some(new String(Base64.getEncoder.encode("Hello Google!".getBytes))),
                    publishTime = ts)

    val pullResponse =
      """{"receivedMessages":[{"ackId":"ack1","message":{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1","publishTime":"2019-07-04T08:10:00.111Z"}}]}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull(config.projectId, "sub1", true, 1000)
    val result =
      Source.single((Done, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", message))))

  }

  it should "Pull with results without access token in emulated mode" in {

    val ts = Instant.parse("2019-07-04T08:10:00.111Z")

    val message =
      PubSubMessage(messageId = "1",
                    data = Some(new String(Base64.getEncoder.encode("Hello Google!".getBytes))),
                    publishTime = ts)

    val pullResponse =
      """{"receivedMessages":[{"ackId":"ack1","message":{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1","publishTime":"2019-07-04T08:10:00.111Z"}}]}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.absent())
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestEmulatorHttpApi.pull(config.projectId, "sub1", true, 1000)
    val result =
      Source.single((Done, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", message))))

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
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull(config.projectId, "sub1", true, 1000)
    val result =
      Source.single((Done, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    result.futureValue shouldBe PullResponse(None)

  }

  it should "Fail pull when HTTP response is error" in {

    val pullResponse = """{"is_valid_json": true}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:pull"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(418).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull(config.projectId, "sub1", true, 1000)
    val result =
      Source.single((Done, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    val failure = result.failed.futureValue
    failure.getMessage should include("418 I'm a teapot")
    failure.getMessage should include(pullResponse)
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
        .withRequestBody(WireMock.equalToJson(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200))
    )

    val acknowledgeRequest = AcknowledgeRequest("ack1")

    val flow = TestHttpApi.acknowledge(config.projectId, "sub1")
    val result =
      Source.single((acknowledgeRequest, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    result.futureValue shouldBe Done

  }

  it should "fail acknowledge when result code is not success" in {
    val ackRequest = """{"ackIds":["ack1"]}"""
    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${config.projectId}/subscriptions/sub1:acknowledge"
          )
        )
        .withRequestBody(WireMock.equalToJson(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(401))
    )

    val acknowledgeRequest = AcknowledgeRequest("ack1")

    val flow = TestHttpApi.acknowledge(config.projectId, "sub1")
    val result =
      Source.single((acknowledgeRequest, Some(accessToken))).via(flow).toMat(Sink.last)(Keep.right).run
    result.failed.futureValue.getMessage should include("401")
  }

  it should "return exception with the meaningful error message in case of not successful publish response" in {
    val publishMessage =
      PublishMessage(
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)),
        attributes = Map("row_id" -> "7")
      )

    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","attributes":{"row_id":"7"}}]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${config.projectId}/topics/topic1:publish")
        )
        .withRequestBody(WireMock.equalToJson(expectedPublishRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
            .withBody("{}")
            .withHeader("Content-Type", "application/json")
        )
    )

    val flow = TestHttpApi.publish[Unit](config.projectId, "topic1", 1)
    val result =
      Source.single((publishRequest, Some(accessToken), ())).via(flow).toMat(Sink.last)(Keep.right).run

    val failure = result.failed.futureValue
    failure shouldBe a[RuntimeException]
    failure.getMessage should include("404")
  }

  private val httpApi = PubSubApi
  if (httpApi.PubSubEmulatorHost.isDefined) it should "honor emulator host variables" in {
    val emulatorVar = sys.props
      .get(httpApi.PubSubEmulatorHostVarName)
      .orElse(sys.env.get(httpApi.PubSubEmulatorHostVarName))

    emulatorVar.foreach { emulatorHost =>
      httpApi.isEmulated shouldBe true
      httpApi.PubSubGoogleApisHost shouldEqual emulatorHost
    }
  }

  override def afterAll(): Unit = {
    wiremockServer.stop()
    Await.result(system.terminate(), 5.seconds)
  }
}
