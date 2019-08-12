/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.storage.impl.GoogleTokenApi.AccessTokenExpiry
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import pdi.jwt.{Jwt, JwtAlgorithm}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleTokenApiSpec extends WordSpecLike with Matchers with ScalaFutures with MockitoSugar with BeforeAndAfterAll {

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  implicit val system: ActorSystem = ActorSystem("GoogleTokenApiSpec")
  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)

  // openssl genrsa -out mykey.pem 1024
  // openssl pkcs8 -topk8 -nocrypt -in mykey.pem -out myrsakey_pcks8
  // openssl rsa -in mykey.pem -pubout > mykey.pub
  val privateKey =
    """-----BEGIN PRIVATE KEY-----
  |MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAMwkmdwrWp+LLlsf
  |bVE+neFjZtUNuaD4/tpQ2UIh2u+qU6sr4bG8PPuqSdrt5b0/0vfMZA11mQWmKpg5
  |PK98kEkhbSvC08fG0TtpR9+vflghOuuvcw6kCniwNbHlOXnE8DwtKQp1DbTUPzMD
  |hhsIjJaUtv19Xk7gh4MqYgANTm6lAgMBAAECgYEAwBXIeHSKxwiNS8ycbg//Oq7v
  |eZV6j077bq0YYLO+cDjSlYOq0DSRJTSsXcXvoE1H00aM9mUq4TfjaGyi/3SzxYsr
  |rSzu/qpYC58MJsnprIjlLgFZmZGe5MOSoul/u6JsBTJGkYPV0xGrtXJY103aSYzC
  |xthpY0BHy9eO9I/pNlkCQQD/64g4INAiBdM4R5iONQvh8LLvqbb8Bw4vVwVFFnAr
  |YHcomxtT9TunMad6KPgbOCd/fTttDADrv54htBrFGXeXAkEAzDTtisPKXPByJnUd
  |jKO2oOg0Fs9IjGeWbnkrsN9j0134ldARE+WbT5S8G5EFo+bQi4ffU3+Y/4ly6Amm
  |OAAzIwJBANV2GAD5HaHDShK/ZTf4dxjWM+pDnSVKnUJPS039EUKdC8cK2RiGjGNA
  |v3jdg1Tw2cE1K8QhJwN8qOFj4JBWVbECQQCwcntej9bnf4vi1wd1YnCHkJyRqQIS
  |7974DhNGfYAQPv5w1JwtCRSuKuJvH1w0R1ijd//scjCNfQKgpNXPRbzpAkAQ8MFA
  |MLpOLGqezUQthJWmVtnXEXaAlb3yFSRTZQVEselObiIc6EvYzNXv780IDT4pyKjg
  |8DS9i5jJDIVWr7mA
  |-----END PRIVATE KEY-----
 """.stripMargin

  val publicKey = """-----BEGIN PUBLIC KEY-----
  |MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDMJJncK1qfiy5bH21RPp3hY2bV
  |Dbmg+P7aUNlCIdrvqlOrK+GxvDz7qkna7eW9P9L3zGQNdZkFpiqYOTyvfJBJIW0r
  |wtPHxtE7aUffr35YITrrr3MOpAp4sDWx5Tl5xPA8LSkKdQ201D8zA4YbCIyWlLb9
  |fV5O4IeDKmIADU5upQIDAQAB
  |-----END PUBLIC KEY-----
 """.stripMargin

  private val settings = TokenApiSettings(
    url = "https://www.googleapis.com/oauth2/v4/token",
    scope = "https://www.googleapis.com/auth/devstorage.read_write"
  )

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

      val api = new GoogleTokenApi(http, settings)
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
      claimsJson should include(""""scope":"https://www.googleapis.com/auth/devstorage.read_write"""")
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

      val api = new GoogleTokenApi(http, settings)
      api.getAccessToken("email", privateKey).futureValue should matchPattern {
        case AccessTokenExpiry("token", exp) if exp > (System.currentTimeMillis / 1000L + 3000L) =>
      }
    }
  }
}
