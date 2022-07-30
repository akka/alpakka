/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.TypesenseHttp.RetryableTypesenseException
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class RetryHttpTypesenseSpec extends AnyFunSpec with BeforeAndAfterEach with should.Matchers with ScalaFutures {
  import RetryHttpTypesenseSpec._

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher
  val port = 8108
  val wireMockServer = new WireMockServer(wireMockConfig().port(port))

  val noRetrySettings: TypesenseSettings = TypesenseSettings(
    s"http://localhost:$port",
    "api_key",
    RetrySettings(maxRetries = 0, minBackoff = 0.second, maxBackoff = 0.second, randomFactor = 0)
  )

  val retrySettings: TypesenseSettings = noRetrySettings.withRetrySettings(
    RetrySettings(maxRetries = 1, minBackoff = 0.second, maxBackoff = 0.second, randomFactor = 0)
  )

  override def beforeEach(): Unit = {
    wireMockServer.start()
  }

  override def afterEach(): Unit = {
    wireMockServer.stop()
  }

  describe("Index document error") {
    val path = s"/collections/companies/documents?action=create"

    def setUpMock() = {
      val okResponse = aResponse().withStatus(200)
      val nokResponse = aResponse().withStatus(500).withBody("Something is wrong")

      wireMockServer.stubFor(
        post(urlEqualTo(path))
          .inScenario("Retry Scenario")
          .whenScenarioStateIs(Scenario.STARTED)
          .willReturn(nokResponse)
          .willSetStateTo("One Attempt")
      )
      wireMockServer.stubFor(
        post(urlEqualTo(path))
          .inScenario("Retry Scenario")
          .whenScenarioStateIs("One Attempt")
          .willReturn(okResponse)
      )
    }

    val source: Source[IndexDocument[Company], NotUsed] = Source.single(
      IndexDocument("companies", Company("d63fc499-80e7-423d-a4cf-b68913a92603", "Functional Corporation"))
    )

    it("should occurs without retries") {
      //set up
      setUpMock()

      //given
      val flow: Flow[IndexDocument[Company], TypesenseResult[Done], Future[NotUsed]] =
        Typesense.indexDocumentFlow[Company](noRetrySettings)

      //when
      val result: Try[TypesenseResult[Done]] =
        source.via(flow).runWith(Sink.head).map(Success.apply).recover(e => Failure(e)).futureValue

      //then
      result shouldBe Failure(new RetryableTypesenseException(StatusCodes.InternalServerError, "Something is wrong"))
    }

    it("should doesn't occur with retries") {
      //set up
      setUpMock()

      //given
      val flow: Flow[IndexDocument[Company], TypesenseResult[Done], Future[NotUsed]] =
        Typesense.indexDocumentFlow[Company](retrySettings)

      //when
      val result: Try[TypesenseResult[Done]] =
        source.via(flow).runWith(Sink.head).map(Success.apply).recover(e => Failure(e)).futureValue

      //then
      result shouldBe Success(new SuccessTypesenseResult(Done))
    }
  }
}

private[http] object RetryHttpTypesenseSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat2(Company)
}
