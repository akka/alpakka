/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.google.cloud.bigquery.impl.GoogleSession
import akka.stream.alpakka.google.cloud.bigquery.impl.sendrequest.EnrichRequestWithOauth.TokenErrorException
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EnrichRequestWithOauthSpec
    extends TestKit(ActorSystem("EnrichRequestWithOauthSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val timeout = Timeout(1.second)

  "EnrichRequestWithOauth" must {

    "Ask token actor, add oauth header" in {
      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.successful("TOKEN")
      val resultF = Source
        .single(HttpRequest())
        .via(EnrichRequestWithOauth(session))
        .runWith(Sink.last)

      val result = Await.result(resultF, 1.second)
      result.headers.head.name() shouldEqual "Authorization"
      result.headers.head.value() shouldEqual "Bearer TOKEN"
    }

    "Token actor response error" in {
      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.failed(TokenErrorException())

      val resultF = Source
        .single(HttpRequest())
        .via(EnrichRequestWithOauth(session))
        .runWith(Sink.last)

      assertThrows[TokenErrorException] {
        Await.result(resultF, 1.second)
      }
    }

  }
}
