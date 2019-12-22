/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import pdi.jwt.{Jwt, JwtAlgorithm}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleTokenApiSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
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

  //http://travistidwell.com/jsencrypt/demo/
  val privateKey =
    """-----BEGIN RSA PRIVATE KEY-----
      |MIIBOgIBAAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V84K5dgzhR9TFpkAp2kl2
      |9BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQJAQVyXbMS7TGDFWnXieKZh
      |Dm/uYA6sEJqheB4u/wMVshjcQdHbi6Rr0kv7dCLbJz2v9bVmFu5i8aFnJy1MJOpA
      |2QIhAPyEAaVfDqJGjVfryZDCaxrsREmdKDlmIppFy78/d8DHAiEAk9JyTHcapckD
      |uSyaE6EaqKKfyRwSfUGO1VJXmPjPDRMCIF9N900SDnTiye/4FxBiwIfdynw6K3dW
      |fBLb6uVYr/r7AiBUu/p26IMm6y4uNGnxvJSqe+X6AxR6Jl043OWHs4AEbwIhANuz
      |Ay3MKOeoVbx0L+ruVRY5fkW+oLHbMGtQ9dZq7Dp9
      |-----END RSA PRIVATE KEY-----""".stripMargin

  val publicKey =
    """-----BEGIN PUBLIC KEY-----
        |MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V
        |84K5dgzhR9TFpkAp2kl29BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQ==
        |-----END PUBLIC KEY-----""".stripMargin

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

      val api = new GoogleTokenApi(http)
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
      decoded.get should include(""""aud":"https://www.googleapis.com/oauth2/v4/token"""")
      decoded.get should include(""""scope":"https://www.googleapis.com/auth/bigquery"""")
      decoded.get should include(""""iss":"email"""")

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

      val api = new GoogleTokenApi(http)
      api.getAccessToken("email", privateKey).futureValue should matchPattern {
        case AccessTokenExpiry("token", exp) if exp > (System.currentTimeMillis / 1000L + 3000L) =>
      }
    }
  }

}
