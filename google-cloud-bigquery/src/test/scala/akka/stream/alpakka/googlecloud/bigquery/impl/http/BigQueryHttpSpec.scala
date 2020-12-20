/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.http

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryException, BigQuerySettings, GoogleOAuth2Exception}
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class BigQueryHttpSpec
    extends TestKit(ActorSystem("BigQueryHttpSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  import BigQueryException._

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  implicit val settings = BigQuerySettings()

  def mockHttp = {
    val http = mock[HttpExt]
    when(http.system) thenReturn system.asInstanceOf[ExtendedActorSystem]
    http
  }

  "BigQueryHttp" must {

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

      val response = BigQueryHttp(http).singleRequestOrFail(HttpRequest())

      assertThrows[BigQueryException](Await.result(response, 1.seconds))
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
        Future.successful(HttpResponse(StatusCodes.OK, Nil, HttpEntity.Empty))
      )

      val response = BigQueryHttp(http).retryRequest(HttpRequest())

      Await.result(response, 3.seconds) should matchPattern {
        case HttpResponse(StatusCodes.OK, Nil, HttpEntity.Empty, _) =>
      }
    }

    "add OAuth header" in {

      val credentialsProvider = mock[CredentialsProvider]
      when(credentialsProvider.getToken()(any[ExecutionContext], any[BigQuerySettings])) thenReturn Future.successful(
        OAuth2BearerToken("TOKEN")
      )
      implicit val settingsWithMockedCredentials = settings.copy(credentialsProvider = credentialsProvider)

      val request = BigQueryHttp().addOAuth(HttpRequest())(settingsWithMockedCredentials)
      Await.result(request, 1.second).headers should matchPattern {
        case HttpHeader("authorization", "Bearer TOKEN") :: Nil =>
      }
    }

    "raise exception" in {

      val credentialsProvider = mock[CredentialsProvider]
      when(credentialsProvider.getToken()(any[ExecutionContext], any[BigQuerySettings])) thenReturn Future.failed(
        GoogleOAuth2Exception(ErrorInfo())
      )
      implicit val settingsWithMockedCredentials = settings.copy(credentialsProvider = credentialsProvider)

      val request = BigQueryHttp().addOAuth(HttpRequest())(settingsWithMockedCredentials)
      assertThrows[GoogleOAuth2Exception] {
        Await.result(request, 1.second)
      }
    }

  }

}
