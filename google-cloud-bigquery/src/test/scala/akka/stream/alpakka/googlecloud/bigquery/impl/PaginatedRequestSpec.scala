/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings, HoverflySupport}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class PaginatedRequestSpec
    extends TestKit(ActorSystem("PaginatedRequestSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit val settings = BigQuerySettings().copy(credentialsProvider = new CredentialsProvider {
    override def projectId: String = ???
    override def getToken()(implicit ec: ExecutionContext, settings: BigQuerySettings): Future[OAuth2BearerToken] =
      Future.successful(OAuth2BearerToken("yyyy.c.an-access-token"))
  })
  implicit val paginated: Paginated[JsValue] = _.asJsObject.fields.get("pageToken").flatMap {
    case JsString(value) => Some(value)
    case _ => None
  }
  val timeout = 3.seconds

  "PaginatedRequest" should {

    "return single page request" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .get("/")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com"), Query.Empty, None)
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.head)

      Await.result(result, timeout) shouldBe JsObject.empty
      hoverfly.reset()
    }

    "return two page request" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .get("/")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(
              success("""{ "pageToken": "nextPage" }""", "application/json")
            )
            .get("/")
            .queryParam("pageToken", "nextPage")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com"), Query.Empty, None)
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.seq)

      Await.result(result, timeout) shouldBe Seq(JsObject("pageToken" -> JsString("nextPage")), JsObject.empty)
      hoverfly.reset()
    }

    "url encode page token" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .get("/")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(
              success("""{ "pageToken": "===" }""", "application/json")
            )
            .get("/")
            .queryParam("pageToken", "===")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com"), Query.Empty, None)
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.seq)

      Await.result(result, timeout) shouldBe Seq(JsObject("pageToken" -> JsString("===")), JsObject.empty)
      hoverfly.reset()
    }
  }

}
