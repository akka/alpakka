/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.ClosedShape
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.SprayJsonSupport._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsObject, JsValue, JsonParser}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ParserSpec extends TestKit(ActorSystem("ParserSpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val ec: ExecutionContext = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val pageToken = "dummyToken"
  val jobId = "dummyJobId"
  val json = JsonParser(s"""{"pageToken": "$pageToken", "jobReference": { "jobId": "$jobId" }, "jobComplete": true}""")
  val jsonWithoutJobId = JsonParser(s"""{"pageToken": "$pageToken"}""")
  val jsonWhereJobIncomplete = JsonParser(
    s"""{"pageToken": "$pageToken", "jobReference": { "jobId": "$jobId" }, "jobComplete": false}"""
  )

  private def createTestGraph[T, Mat1, Mat2](
      source: Source[JsValue, Any],
      dataSink: Sink[T, Mat1],
      pageSink: Sink[PagingInfo, Mat2]
  )(implicit unmarshaller: Unmarshaller[JsValue, T]): RunnableGraph[(Mat1, Mat2)] =
    RunnableGraph.fromGraph(GraphDSL.create(dataSink, pageSink)((_, _)) { implicit builder => (s1, s2) =>
      import GraphDSL.Implicits._

      val parser = builder.add(Parser[T, JsValue])

      source ~> parser.in
      parser.out0 ~> s1
      parser.out1 ~> s2

      ClosedShape
    })

  "Parser" should {

    "output the value returned by the parse function" in {
      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "the value")
      val testGraph =
        createTestGraph(Source.single(json), TestSink.probe[String], TestSink.probe[PagingInfo])

      val (dataSink, pageSink) = testGraph.run()

      Future(pageSink.requestNext())

      val result = dataSink.requestNext(3.seconds)

      result shouldBe "the value"
    }

    "output job id/status and page token parsed from the http response" in {
      val testGraph =
        createTestGraph(Source.single(json), TestSink.probe[JsValue], TestSink.probe[PagingInfo])

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())

      val pagingInfo = pageSink.requestNext(3.seconds)

      pagingInfo shouldBe PagingInfo(Some(JobInfo(JobReference(None, Some(jobId), None), jobComplete = true)),
                                     Some(pageToken))
    }

    "output the page token parsed from the http response" in {
      implicit val unmarshaller = Unmarshaller.strict((x: JsValue) => x.asJsObject)
      val testGraph =
        createTestGraph(Source.single(jsonWithoutJobId), TestSink.probe[JsObject], TestSink.probe[PagingInfo])

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())

      val pagingInfo = pageSink.requestNext(3.seconds)

      pagingInfo shouldBe PagingInfo(None, Some(pageToken))
    }

    "output none for page token when http response does not contain page token" in {
      val testGraph =
        createTestGraph(Source.single(JsObject()), TestSink.probe[JsValue], TestSink.probe[PagingInfo])

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())

      val pagingInfo = pageSink.requestNext(3.seconds)

      pagingInfo shouldBe PagingInfo(None, None)
    }

    "output job status when http response indicates job is incomplete" in {
      val testGraph =
        createTestGraph(Source.single(jsonWhereJobIncomplete), TestSink.probe[JsValue], TestSink.probe[PagingInfo])

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())

      val pagingInfo = pageSink.requestNext(3.seconds)

      pagingInfo shouldBe PagingInfo(Some(JobInfo(JobReference(None, Some(jobId), None), jobComplete = false)),
                                     Some(pageToken))
    }

  }
}
