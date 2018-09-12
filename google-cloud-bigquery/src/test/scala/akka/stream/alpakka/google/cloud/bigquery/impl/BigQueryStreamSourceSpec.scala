/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{when, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class BigQueryStreamSourceSpec
    extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  val timeout = 3.seconds
  implicit val materializer: Materializer = ActorMaterializer()

  trait Scope {
    val session = mock[GoogleSession]
    when(session.getToken()) thenReturn Future.successful("TOKEN")

    val http = mock[HttpExt]

    def checkUsedToken(expected: String) = {
      import org.mockito.ArgumentCaptor

      import collection.JavaConverters._
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

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success", session, http)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      Await.result(resultF, timeout) shouldBe "success"
      verify(session).getToken()
      checkUsedToken("TOKEN")
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
                    entity = HttpEntity("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }""")
                  )
                )
              case "/job123?pageToken=nextPage" =>
                Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
            }
          }
        }
      )

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success", session, http)

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      verify(session, times(2)).getToken()
      checkUsedToken("TOKEN")
    }

    "url encode page token" in new Scope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success", session, http)
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
                  HttpResponse(entity = HttpEntity("""{ "pageToken": "===", "jobReference": { "jobId": "job123"} }"""))
                )
              case "/job123?pageToken=%3D%3D%3D" => Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
            }
          }
        }
      )

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      verify(session, times(2)).getToken()
      checkUsedToken("TOKEN")
    }

  }

}
