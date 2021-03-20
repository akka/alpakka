/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.alpakka.google.{GoogleSettings, TestGoogleSettings}
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.GoogleHttpException
import akka.stream.alpakka.google.auth.{Credentials, GoogleOAuth2Exception}
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import spray.json.{JsObject, JsValue}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleHttpSpec
    extends TestKit(ActorSystem("GoogleHttpSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestGoogleSettings
    with MockitoSugar {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def mockHttp = {
    val http = mock[HttpExt]
    when(http.system) thenReturn system.asInstanceOf[ExtendedActorSystem]
    http
  }

  "GoogleHttp" must {

    "handle unexpected http error" in {

      val http = mockHttp
      when(
        http.singleRequest(
          any[HttpRequest],
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn Future.successful(
        HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))
      )

      import GoogleHttpException._
      val response = GoogleHttp(http).singleRequest[JsValue](HttpRequest())

      assertThrows[GoogleHttpException](Await.result(response, 1.seconds))
    }

    "retry failed request" in {

      val http = mockHttp
      when(
        http.singleRequest(
          any[HttpRequest],
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn (
        Future.successful(
          HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))
        ),
        Future.successful(HttpResponse(StatusCodes.OK, Nil, HttpEntity(ContentTypes.`application/json`, "{}")))
      )

      import GoogleHttpException._
      val response = GoogleHttp(http).singleRequest[JsValue](HttpRequest())

      Await.result(response, 3.seconds) should matchPattern {
        case JsObject.empty =>
      }
    }

    "add OAuth header" in {

      val credentials = mock[Credentials]
      when(credentials.getToken()(any[ExecutionContext], any[GoogleSettings])) thenReturn Future.successful(
        OAuth2BearerToken("TOKEN")
      )
      implicit val settingsWithMockedCredentials = settings.copy(credentials = credentials)

      val request = GoogleHttp().addAuth(HttpRequest())(settingsWithMockedCredentials)
      Await.result(request, 1.second).headers should matchPattern {
        case HttpHeader("authorization", "Bearer TOKEN") :: Nil =>
      }
    }

    "raise exception" in {

      val credentials = mock[Credentials]
      when(credentials.getToken()(any[ExecutionContext], any[GoogleSettings])) thenReturn Future.failed(
        GoogleOAuth2Exception(ErrorInfo())
      )
      implicit val settingsWithMockedCredentials = settings.copy(credentials = credentials)

      val request = GoogleHttp().addAuth(HttpRequest())(settingsWithMockedCredentials)
      assertThrows[GoogleOAuth2Exception] {
        Await.result(request, 1.second)
      }
    }

  }

}
