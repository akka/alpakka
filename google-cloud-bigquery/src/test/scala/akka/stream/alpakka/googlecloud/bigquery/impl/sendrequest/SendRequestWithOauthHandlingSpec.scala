/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class SendRequestWithOauthHandlingSpec
    extends TestKit(ActorSystem("SendRequestWithOauthHandling"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(1.second)

  "SendRequestWithOauthHandling" must {

    "handle unexpected http error" in {

      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.successful("TOKEN")

      val http = mock[HttpExt]
      when(
        http.singleRequest(
          HttpRequest()
            .addHeader(Authorization(OAuth2BearerToken("TOKEN")))
        )
      ) thenReturn Future.successful(HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity("my custom error")))

      val resultF = Source
        .single(HttpRequest())
        .via(SendRequestWithOauthHandling(session, http))
        .runWith(Sink.last)

      val result = Try(Await.result(resultF, 1.second))
      result.toString shouldEqual Failure(
        new IllegalStateException(s"Unexpected error in response: 500 Internal Server Error, my custom error")
      ).toString

    }

  }
}
