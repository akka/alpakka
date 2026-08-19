/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Clock
import scala.concurrent.ExecutionContext

class GoogleOAuth2ExceptionSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit val defaultPatience: PatienceConfig = PatienceConfig(remainingOrDefault)
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val clock: Clock = Clock.systemUTC()

  private val uri = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

  "unmarshalOrFail" should {

    "unmarshal a successful response" in {
      val response = HttpResponse(
        entity = HttpEntity(ContentTypes.`application/json`,
                            """{"access_token": "token", "token_type": "String", "expires_in": 3600}""")
      )

      GoogleOAuth2Exception.unmarshalOrFail[AccessToken](uri, response).futureValue should matchPattern {
        case AccessToken("token", _) =>
      }
    }

    "report the status of an error response with an empty body" in {
      val response = HttpResponse(StatusCodes.ServiceUnavailable, entity = HttpEntity.Empty)

      val ex = GoogleOAuth2Exception.unmarshalOrFail[AccessToken](uri, response).failed.futureValue
      ex shouldBe a[GoogleOAuth2Exception]
      ex.getMessage should include("503 Service Unavailable")
      ex.getMessage should include(uri)
    }

    "report the body of an error response" in {
      val response = HttpResponse(StatusCodes.Forbidden, entity = HttpEntity("service account not enabled"))

      val ex = GoogleOAuth2Exception.unmarshalOrFail[AccessToken](uri, response).failed.futureValue
      ex shouldBe a[GoogleOAuth2Exception]
      ex.getMessage should include("403 Forbidden")
      ex.getMessage should include("service account not enabled")
    }
  }
}
