/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.huawei.pushkit.HmsSettings
import akka.stream.alpakka.huawei.pushkit.models.{ErrorResponse, PushKitNotification, PushKitResponse}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Await, ExecutionContext, Future}

class PushKitSenderSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {

  import PushKitJsonSupport._

  override def afterAll() =
    TestKit.shutdownActorSystem(system)

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val config: HmsSettings = HmsSettings()

  "HmsSender" should {

    "call the api as the docs want to" in {
      val sender = new PushKitSender
      val http = mock(classOf[HttpExt])
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]()
        )
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity =
              HttpEntity(ContentTypes.`application/json`, """{"code": "", "msg": "", "requestId": ""}""".stripMargin)
          )
        )
      )

      Await.result(sender.send(config, "token", http, PushKitSend(false, PushKitNotification.empty), system),
                   defaultPatience.timeout
      )

      val captor: ArgumentCaptor[HttpRequest] = ArgumentCaptor.forClass(classOf[HttpRequest])
      verify(http).singleRequest(captor.capture(),
                                 any[HttpsConnectionContext](),
                                 any[ConnectionPoolSettings](),
                                 any[LoggingAdapter]()
      )
      val request: HttpRequest = captor.getValue
      Unmarshal(request.entity).to[PushKitSend].futureValue shouldBe PushKitSend(false, PushKitNotification.empty)
      request.uri.toString shouldBe "https://push-api.cloud.huawei.com/v1/" + config.appId + "/messages:send"
      request.headers.size shouldBe 1
      request.headers.head should matchPattern { case HttpHeader("authorization", "Bearer token") => }
    }

    "parse the success response correctly" in {
      val sender = new PushKitSender
      val http = mock(classOf[HttpExt])
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]()
        )
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(ContentTypes.`application/json`,
                                """{"code": "80000000", "msg": "Success", "requestId": "1357"}"""
            )
          )
        )
      )

      sender
        .send(config, "token", http, PushKitSend(false, PushKitNotification.empty), system)
        .futureValue shouldBe PushKitResponse("80000000", "Success", "1357")
    }

    "parse the error response correctly" in {
      val sender = new PushKitSender
      val http = mock(classOf[HttpExt])
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]()
        )
      ).thenReturn(
        Future.successful(
          HttpResponse(
            status = StatusCodes.ServiceUnavailable,
            entity = HttpEntity(ContentTypes.`application/json`,
                                """{"code": "80100003", "msg": "Illegal payload", "requestId": "1357"}"""
            )
          )
        )
      )

      sender
        .send(config, "token", http, PushKitSend(false, PushKitNotification.empty), system)
        .futureValue shouldBe ErrorResponse("""{"code":"80100003","msg":"Illegal payload","requestId":"1357"}""")
    }
  }
}
