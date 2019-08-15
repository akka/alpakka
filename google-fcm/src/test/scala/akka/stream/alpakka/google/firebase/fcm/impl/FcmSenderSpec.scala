/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.google.firebase.fcm.{FcmErrorResponse, FcmSuccessResponse}
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.testkit.TestKit
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class FcmSenderSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with MockitoSugar
    with BeforeAndAfterAll {

  import FcmJsonSupport._

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val materializer = ActorMaterializer()

  "FcmSender" should {

    "call the api as the docs want to" in {
      val sender = new FcmSender
      val http = mock[HttpExt]
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, """{"name": ""}""")))
      )

      Await.result(sender.send("projectId", "token", http, FcmSend(false, FcmNotification.empty)),
                   defaultPatience.timeout)

      val captor: ArgumentCaptor[HttpRequest] = ArgumentCaptor.forClass(classOf[HttpRequest])
      verify(http).singleRequest(captor.capture(),
                                 any[HttpsConnectionContext](),
                                 any[ConnectionPoolSettings](),
                                 any[LoggingAdapter]())
      val request: HttpRequest = captor.getValue
      Unmarshal(request.entity).to[FcmSend].futureValue shouldBe FcmSend(false, FcmNotification.empty)
      request.uri.toString shouldBe "https://fcm.googleapis.com/v1/projects/projectId/messages:send"
      request.headers.size shouldBe 1
      request.headers.head should matchPattern { case HttpHeader("authorization", "Bearer token") => }
    }

    "parse the success response correctly" in {
      val sender = new FcmSender
      val http = mock[HttpExt]
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, """{"name": "test"}""")))
      )

      sender
        .send("projectId", "token", http, FcmSend(false, FcmNotification.empty))
        .futureValue shouldBe FcmSuccessResponse("test")
    }

    "parse the error response correctly" in {
      val sender = new FcmSender
      val http = mock[HttpExt]
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(
          HttpResponse(status = StatusCodes.BadRequest,
                       entity = HttpEntity(ContentTypes.`application/json`, """{"name":"test"}"""))
        )
      )

      sender
        .send("projectId", "token", http, FcmSend(false, FcmNotification.empty))
        .futureValue shouldBe FcmErrorResponse(
        """{"name":"test"}"""
      )
    }

  }
}
