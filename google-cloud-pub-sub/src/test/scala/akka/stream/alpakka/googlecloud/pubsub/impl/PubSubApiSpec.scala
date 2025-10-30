/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, urlEqualTo}
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.cert.X509Certificate
import java.time.Instant
import java.util.Base64
import javax.net.ssl.{SSLContext, SSLEngine, X509TrustManager}
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

class PubSubApiSpec extends AnyFlatSpec with BeforeAndAfterAll with ScalaFutures with Matchers with LogCapturing {

  implicit val system: ActorSystem = ActorSystem(
    "PubSubApiSpec",
    ConfigFactory
      .parseString(
        s"alpakka.google.credentials.none.project-id = ${TestCredentials.projectId}"
      )
      .withFallback(ConfigFactory.load())
  )

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  def createInsecureSslEngine(host: String, port: Int): SSLEngine = {
    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(null, Array(new NoopTrustManager()), null)

    val engine = sslContext.createSSLEngine(host, port)
    engine.setUseClientMode(true)

    engine
  }

  Http().setDefaultClientHttpsContext(ConnectionContext.httpsClient(createInsecureSslEngine _))

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

  val config = PubSubConfig()

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
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?prettyPrint=false")
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
    val flow = TestHttpApi.publish[Unit]("topic1", 1)
    val result =
      Source.single((publishRequest, ())).via(flow).toMat(Sink.head)(Keep.right).run()
    result.futureValue._1.messageIds shouldBe Seq("1")
    result.futureValue._2 shouldBe (())
  }

  it should "publish with ordering key" in {

    val publishMessage =
      PublishMessage(
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)),
        attributes = Some(Map("row_id" -> "7")),
        orderingKey = Some("my-ordering-key")
      )
    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ==","attributes":{"row_id":"7"},"orderingKey":"my-ordering-key"}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?prettyPrint=false")
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
    val flow = TestHttpApi.publish[Unit]("topic1", 1)
    val result =
      Source.single((publishRequest, ())).via(flow).toMat(Sink.head)(Keep.right).run()
    result.futureValue._1.messageIds shouldBe Seq("1")
    result.futureValue._2 shouldBe (())
  }

  it should "publish to overridden host" in {
    val httpApiWithHostToOverride = new PubSubApi {
      val isEmulated = false
      val PubSubGoogleApisHost = "invalid-host" //this host must be override to complete the test
      val PubSubGoogleApisPort = wiremockServer.httpsPort()
    }

    val publishMessage =
      PublishMessage(
        data = new String(Base64.getEncoder.encode("Hello Google!".getBytes))
      )

    val publishRequest = PublishRequest(Seq(publishMessage))

    val expectedPublishRequest =
      """{"messages":[{"data":"SGVsbG8gR29vZ2xlIQ=="}]}"""
    val publishResponse = """{"messageIds":["1"]}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?prettyPrint=false")
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
    val flow = httpApiWithHostToOverride.publish[Unit]("topic1", 1, Some("localhost"))
    val result =
      Source.single((publishRequest, ())).via(flow).toMat(Sink.head)(Keep.right).run()
    result.futureValue._1.messageIds shouldBe Seq("1")
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
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?prettyPrint=false")
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

    val flow = TestEmulatorHttpApi.publish[Unit]("topic1", 1)
    val result =
      Source.single((publishRequest, ())).via(flow).toMat(Sink.last)(Keep.right).run()
    result.futureValue._1.messageIds shouldBe Seq("1")
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
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull("sub1", true, 1000)
    val result = Source.single(Done).via(flow).toMat(Sink.last)(Keep.right).run()
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", message))))

  }

  it should "Pull with results with ordering key" in {

    val ts = Instant.parse("2019-07-04T08:10:00.111Z")

    val message =
      PubSubMessage(messageId = "1",
                    data = Some(new String(Base64.getEncoder.encode("Hello Google!".getBytes))),
                    publishTime = ts,
                    orderingKey = Some("my-ordering-key"))

    val pullResponse =
      """{"receivedMessages":[{"ackId":"ack1","message":{"data":"SGVsbG8gR29vZ2xlIQ==","messageId":"1","publishTime":"2019-07-04T08:10:00.111Z","orderingKey":"my-ordering-key"}}]}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull("sub1", true, 1000)
    val result = Source.single(Done).via(flow).toMat(Sink.last)(Keep.right).run()
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
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.absent())
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestEmulatorHttpApi.pull("sub1", true, 1000)
    val result =
      Source.single(Done).via(flow).toMat(Sink.last)(Keep.right).run()
    result.futureValue shouldBe PullResponse(Some(Seq(ReceivedMessage("ack1", message))))

  }

  it should "Pull without results" in {

    val pullResponse = "{}"

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull("sub1", true, 1000)
    val result =
      Source.single(Done).via(flow).toMat(Sink.last)(Keep.right).run()
    result.futureValue shouldBe PullResponse(None)

  }

  it should "Fail pull when HTTP response is error" in {

    val pullResponse = """{"is_valid_json": true}"""

    val pullRequest = """{"returnImmediately":true,"maxMessages":1000}"""

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:pull?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(pullRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(418).withBody(pullResponse).withHeader("Content-Type", "application/json"))
    )

    val flow = TestHttpApi.pull("sub1", true, 1000)
    val result =
      Source.single(Done).via(flow).toMat(Sink.last)(Keep.right).run()
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
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:acknowledge?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(200))
    )

    val acknowledgeRequest = AcknowledgeRequest("ack1")

    val flow = TestHttpApi.acknowledge("sub1")
    val result =
      Source.single(acknowledgeRequest).via(flow).toMat(Sink.last)(Keep.right).run()
    result.futureValue shouldBe Done

  }

  it should "fail acknowledge when result code is not success" in {
    val ackRequest = """{"ackIds":["ack1"]}"""
    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/v1/projects/${TestCredentials.projectId}/subscriptions/sub1:acknowledge?prettyPrint=false"
          )
        )
        .withRequestBody(WireMock.equalToJson(ackRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + accessToken))
        .willReturn(aResponse().withStatus(401))
    )

    val acknowledgeRequest = AcknowledgeRequest("ack1")

    val flow = TestHttpApi.acknowledge("sub1")
    val result =
      Source.single(acknowledgeRequest).via(flow).toMat(Sink.last)(Keep.right).run()
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
          urlEqualTo(s"/v1/projects/${TestCredentials.projectId}/topics/topic1:publish?prettyPrint=false")
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

    val flow = TestHttpApi.publish[Unit]("topic1", 1)
    val result =
      Source.single((publishRequest, ())).via(flow).toMat(Sink.last)(Keep.right).run()

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
