/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.NotUsed
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, OK}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.alpakka.google.auth.{Credentials, GoogleOAuth2Exception}
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.{GoogleHttpException, GoogleSettings, RequestSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.{any, anyInt, argThat}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import spray.json.{JsObject, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class GoogleHttpSpec
    extends TestKit(ActorSystem("GoogleHttpSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with MockitoSugar {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def mockHttp: HttpExt = {
    val http = mock[HttpExt]
    when(http.system) thenReturn system.asInstanceOf[ExtendedActorSystem]
    http
  }

  def mockHttp(response: Source[HttpResponse, NotUsed]): HttpExt = {
    val http = mockHttp
    when(
      http.cachedHostConnectionPoolHttps(
        any[String],
        anyInt,
        any[HttpsConnectionContext],
        any[ConnectionPoolSettings],
        any[LoggingAdapter]
      )
    ) thenReturn (Flow[Any]
      .zipWith(response)(Keep.right)
      .map(Try(_))
      .map((_, mock[Nothing]))
      .mapMaterializedValue(_ => mock[HostConnectionPool]), Nil: _*): @nowarn("msg=dead code")
    http
  }

  implicit val settings = GoogleSettings().requestSettings

  "GoogleHttp" must {

    "throw for bad request without retrying" in {

      val response1 = HttpResponse(BadRequest, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))
      val response2 = HttpResponse(OK, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))

      val http = mockHttp(Source(List(response1, response2)))
      when(
        http.singleRequest(
          any[HttpRequest],
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn (
        Future.successful(response1),
        Future.successful(response2)
      )

      import GoogleHttpException._

      val result1 = GoogleHttp(http).singleRequest[JsValue](HttpRequest())
      assert(result1.failed.futureValue.isInstanceOf[GoogleHttpException])

      val result2 = Source
        .single(HttpRequest())
        .via(GoogleHttp(http).cachedHostConnectionPool[JsValue](""))
        .runWith(Sink.head)
      assert(result2.failed.futureValue.isInstanceOf[GoogleHttpException])
    }

    "retry internal server error" in {

      val response1 = HttpResponse(InternalServerError, Nil, HttpEntity(`application/json`, "{}"))
      val response2 = HttpResponse(OK, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))

      val http = mockHttp(Source(List(response1, response2)))
      when(
        http.singleRequest(
          any[HttpRequest],
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn (Future.successful(response1), Future.successful(response2))

      import GoogleHttpException._
      val result1 = GoogleHttp(http).singleRequest[JsValue](HttpRequest())
      result1.futureValue should matchPattern {
        case JsObject.empty =>
      }

      val result2 = Source
        .single(HttpRequest())
        .via(GoogleHttp(http).cachedHostConnectionPool[JsValue](""))
        .runWith(Sink.head)
      result2.futureValue should matchPattern {
        case JsObject.empty =>
      }
    }

    "add Authorization header" in {

      val http = mockHttp
      when(
        http.singleRequest(
          argThat[HttpRequest](_.headers.contains(Authorization(OAuth2BearerToken("yyyy.c.an-access-token")))),
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn Future.successful(HttpResponse(OK, Nil, HttpEntity(ContentTypes.`application/json`, "{}")))

      import GoogleHttpException._
      val response = GoogleHttp(http).singleAuthenticatedRequest[JsValue](HttpRequest())

      response.futureValue should matchPattern {
        case JsObject.empty =>
      }

    }

    "raise exception without retrying if auth request fails" in {

      final class AnotherException extends RuntimeException

      val credentials = mock[Credentials]
      when(credentials.get()(any[ExecutionContext], any[RequestSettings])) thenReturn (
        Future.failed(GoogleOAuth2Exception(ErrorInfo())),
        Future.failed(new AnotherException),
      )
      implicit val settingsWithMockedCredentials = GoogleSettings().copy(credentials = credentials)

      val http = mockHttp
      when(
        http.singleRequest(
          any[HttpRequest],
          any[HttpsConnectionContext],
          any[ConnectionPoolSettings],
          any[LoggingAdapter]
        )
      ) thenReturn (
        Future.successful(HttpResponse(BadRequest, Nil, HttpEntity(ContentTypes.`application/json`, "{}"))),
        Future.successful(HttpResponse(OK, Nil, HttpEntity(ContentTypes.`application/json`, "{}")))
      )

      import GoogleHttpException._
      val response = GoogleHttp(http).singleAuthenticatedRequest[JsValue](HttpRequest())

      assert(response.failed.futureValue.isInstanceOf[GoogleOAuth2Exception])
    }

  }

}
