/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.alpakka.googlecloud.bigquery.e2e.BigQueryTableHelper
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryCallbacks
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.SprayJsonSupport._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{when, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import spray.json.JsValue

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class BigQueryStreamSourceSpec
    extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BigQueryTableHelper
    with MockitoSugar {

  override implicit val actorSystem: ActorSystem = ActorSystem("BigQueryEndToEndSpec")
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val timeout = 3.seconds

  trait Scope {

    val http = mock[HttpExt]

    def checkUsedToken(expected: String) = {
      import org.mockito.ArgumentCaptor

      val argument: ArgumentCaptor[HttpRequest] = ArgumentCaptor.forClass(classOf[HttpRequest])
      verify(http, atLeastOnce).singleRequest(argument.capture,
                                              any[HttpsConnectionContext](),
                                              any[ConnectionPoolSettings](),
                                              any[LoggingAdapter]())
      argument.getAllValues.asScala.last.headers.head.value().stripPrefix("Bearer ") shouldBe expected
    }
  }

  "BigQueryStreamSource" should {

    "return single page request" in new Scope {

      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(ContentTypes.`application/json`, """{}""")
          )
        )
      )

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")

      val bigQuerySource =
        BigQueryStreamSource[JsValue, String](HttpRequest(), BigQueryCallbacks.ignore, projectConfig, http)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      Await.result(resultF, timeout) shouldBe "success"
      checkUsedToken("yyyy.c.an-access-token")
    }

    "return two page request" in new Scope {

      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenAnswer(
        new Answer[Future[HttpResponse]] {
          override def answer(invocation: InvocationOnMock): Future[HttpResponse] = {
            val request = invocation.getArguments()(0).asInstanceOf[HttpRequest]
            request.uri.toString() match {
              case "/" =>
                Future.successful(
                  HttpResponse(
                    entity = HttpEntity(ContentTypes.`application/json`,
                                        """{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }""")
                  )
                )
              case "/job123?pageToken=nextPage" =>
                Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, """{}""")))
            }
          }
        }
      )

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), BigQueryCallbacks.ignore, projectConfig, http)

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      checkUsedToken("yyyy.c.an-access-token")
    }

    "url encode page token" in new Scope {

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), BigQueryCallbacks.ignore, projectConfig, http)
      when(
        http.singleRequest(any[HttpRequest](),
                           any[HttpsConnectionContext](),
                           any[ConnectionPoolSettings](),
                           any[LoggingAdapter]())
      ).thenAnswer(
        new Answer[Future[HttpResponse]] {
          override def answer(invocation: InvocationOnMock): Future[HttpResponse] = {
            val request = invocation.getArguments()(0).asInstanceOf[HttpRequest]
            request.uri.toString() match {
              case "/" =>
                Future.successful(
                  HttpResponse(
                    entity = HttpEntity(ContentTypes.`application/json`,
                                        """{ "pageToken": "===", "jobReference": { "jobId": "job123"} }""")
                  )
                )
              case "/job123?pageToken=%3D%3D%3D" =>
                Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, """{}""")))
            }
          }
        }
      )

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      checkUsedToken("yyyy.c.an-access-token")
    }

  }

}
