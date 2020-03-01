/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleTokenApi.AccessTokenExpiry
import akka.testkit.TestKit
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import pdi.jwt.{Jwt, JwtAlgorithm}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

class GoogleTokenApiSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with MockitoSugar
    with BeforeAndAfterAll {

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val materializer = ActorMaterializer()

  lazy val privateKey = {
    Source.fromResource("private_pcks8.pem").getLines().mkString("\n").stripMargin
  }

  lazy val publicKey = {
    Source.fromResource("key.pub").getLines().mkString("\n").stripMargin
  }

  "GoogleTokenApi" should {

    "call the api as the docs want to" in {
      val http = mock[HttpExt]
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(ContentTypes.`application/json`,
                                """{"access_token": "token", "token_type": "String", "expires_in": 3600}""")
          )
        )
      )

      val api = new GoogleTokenApi(http, system, Option.empty)
      Await.result(api.getAccessToken("email", privateKey), defaultPatience.timeout)

      val captor: ArgumentCaptor[HttpRequest] = ArgumentCaptor.forClass(classOf[HttpRequest])
      verify(http).singleRequest(captor.capture(),
                                 any[HttpsConnectionContext](),
                                 any[ConnectionPoolSettings](),
                                 any[LoggingAdapter]())
      val request: HttpRequest = captor.getValue
      request.uri.toString shouldBe "https://www.googleapis.com/oauth2/v4/token"
      val data = Unmarshal(request.entity).to[String].futureValue
      data should startWith("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=")
      val jwt = data.replace("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=", "")
      val decoded = Jwt.decode(jwt, publicKey, Seq(JwtAlgorithm.RS256))
      decoded.isSuccess shouldBe true
      val claimsJson = decoded.get.toJson
      claimsJson should include(""""aud":"https://www.googleapis.com/oauth2/v4/token"""")
      claimsJson should include(""""scope":"https://www.googleapis.com/auth/bigquery"""")
      claimsJson should include(""""iss":"email"""")
    }

    "return the token" in {
      val http = mock[HttpExt]
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(ContentTypes.`application/json`,
                                """{"access_token": "token", "token_type": "String", "expires_in": 3600}""")
          )
        )
      )

      val api = new GoogleTokenApi(http, system, Option.empty)
      api.getAccessToken("email", privateKey).futureValue should matchPattern {
        case AccessTokenExpiry("token", exp) if exp > (System.currentTimeMillis / 1000L + 3000L) =>
      }
    }
  }

}
