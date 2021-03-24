/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.duration._

class PaginatedRequestSpec
    extends TestKit(ActorSystem("PaginatedRequestSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with HoverflySupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit val patience = PatienceConfig(remainingOrDefault)
  implicit val paginated: Paginated[JsValue] = _.asJsObject.fields.get("pageToken").flatMap {
    case JsString(value) => Some(value)
    case _ => None
  }

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

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com")).runWith(Sink.head)

      result.futureValue shouldBe JsObject.empty
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

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com")).runWith(Sink.seq)

      result.futureValue shouldBe Seq(JsObject("pageToken" -> JsString("nextPage")), JsObject.empty)
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

      val result = PaginatedRequest[JsValue](HttpRequest(GET, "https://example.com")).runWith(Sink.seq)

      result.futureValue shouldBe Seq(JsObject("pageToken" -> JsString("===")), JsObject.empty)
      hoverfly.reset()
    }
  }

}
