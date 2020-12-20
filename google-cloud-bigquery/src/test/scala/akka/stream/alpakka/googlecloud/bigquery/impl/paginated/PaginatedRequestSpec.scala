/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.impl.paginated
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.SprayJsonSupport._
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings, HoverflySupport}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.JsValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class PaginatedRequestSpec
    extends TestKit(ActorSystem("PaginatedRequestSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit val settings = BigQuerySettings().copy(credentialsProvider = new CredentialsProvider {
    override def projectId: String = ???
    override def getToken()(implicit ec: ExecutionContext, settings: BigQuerySettings): Future[OAuth2BearerToken] =
      Future.successful(OAuth2BearerToken("yyyy.c.an-access-token"))
  })
  val timeout = 3.seconds

  "PaginatedRequest" should {

    "return single page request" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .post("/")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")
      val result = PaginatedRequest[String, JsValue](HttpRequest(POST, "https://example.com"))
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.head)

      Await.result(result, timeout) shouldBe "success"
      hoverfly.reset()
    }

    "return two page request" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .post("/")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(
              success("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123" }, "jobComplete": true }""",
                      "application/json")
            )
            .get("/job123")
            .queryParam("pageToken", "nextPage")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")
      val result = paginated
        .PaginatedRequest[String, JsValue](HttpRequest(POST, "https://example.com"))
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.seq)

      Await.result(result, timeout) shouldBe Seq("success", "success")
      hoverfly.reset()
    }

    "url encode page token" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .post("/")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(
              success("""{ "pageToken": "===", "jobReference": { "jobId": "job123" }, "jobComplete": true }""",
                      "application/json")
            )
            .get("/job123")
            .queryParam("pageToken", "===")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .willReturn(success("{}", "application/json"))
        )
      )

      implicit val unmarshaller = Unmarshaller.strict((_: JsValue) => "success")
      val result = paginated
        .PaginatedRequest[String, JsValue](HttpRequest(POST, "https://example.com"))
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.seq)

      Await.result(result, timeout) shouldBe Seq("success", "success")
      hoverfly.reset()
    }
  }

  "EndOfStreamDetector" should {

    val jobReference = JobReference(None, Some("jobId"), None)
    val completeJob = JobInfo(jobReference, jobComplete = true)
    val incompleteJob = JobInfo(jobReference, jobComplete = false)
    val pageToken = "next page"

    "terminate processing if job is complete and page token is none" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val probe = Source
        .single(PagingInfo(Some(completeJob), None))
        .via(PaginatedRequest.EndOfStreamDetector)
        .runWith(sinkProbe)

      probe.expectSubscriptionAndComplete()
    }

    "terminate processing if not a job and page token is none" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val probe = Source
        .single(PagingInfo(None, None))
        .via(PaginatedRequest.EndOfStreamDetector)
        .runWith(sinkProbe)

      probe.expectSubscriptionAndComplete()
    }

    "forward input if job is incomplete, no matter the page token" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val first = PagingInfo(Some(incompleteJob), None)
      val second = PagingInfo(Some(incompleteJob), Some(pageToken))

      val probe = Source(List(first, second))
        .via(PaginatedRequest.EndOfStreamDetector)
        .runWith(sinkProbe)

      probe.requestNext() should be(first)
      probe.requestNext() should be(second)
    }

    "forward input if page token exists when job is complete" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val pageInfo = PagingInfo(Some(completeJob), Some(pageToken))

      val probe = Source
        .single(pageInfo)
        .via(PaginatedRequest.EndOfStreamDetector)
        .runWith(sinkProbe)

      probe.requestNext() should be(pageInfo)
    }

    "forward input if page token exists, even if not a job" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val pageInfo = PagingInfo(None, Some(pageToken))

      val probe = Source
        .single(pageInfo)
        .via(PaginatedRequest.EndOfStreamDetector)
        .runWith(sinkProbe)

      probe.requestNext() should be(pageInfo)
    }

  }

  "FlowInitializer" should {

    "put initial value in front of the stream" in {
      val sourceProbe = TestSource.probe[PagingInfo]
      val sinkProbe = TestSink.probe[Option[PagingInfo]]

      val probe = sourceProbe
        .via(PaginatedRequest.FlowInitializer)
        .runWith(sinkProbe)

      probe.requestNext() should be(None)
    }
  }

//  "JobReferenceMaterializer" should {
//
//    "succeed with JobReference if job flows through" in {
//
//      val pagingInfos = List(PagingInfo(Some(JobInfo(JobReference(None, Some("jobId"), None), jobComplete = false)), None),
//                             PagingInfo(None, None),
//                             PagingInfo(None, None))
//      val (jobReference, elements) = Source(pagingInfos)
//        .viaMat(PaginatedRequest.JobReferenceMaterializer)(Keep.right)
//        .toMat(Sink.seq)(Keep.both)
//        .run()
//
//      Await.result(jobReference, 1.second) should matchPattern {
//        case Some(JobReference(Some("jobId"))) =>
//      }
//      Await.result(elements, 1.second) should equal(pagingInfos)
//    }
//
//    "succeed with None if no job flows through" in {
//
//      val jobReference = Source
//        .single(PagingInfo(None, None))
//        .viaMat(PaginatedRequest.JobReferenceMaterializer)(Keep.right)
//        .to(Sink.ignore)
//        .run()
//
//      Await.result(jobReference, 1.second) should matchPattern {
//        case None =>
//      }
//    }
//
//    "fail if nothing flows through" in {
//
//      val jobReference = Source.empty
//        .viaMat(PaginatedRequest.JobReferenceMaterializer)(Keep.right)
//        .to(Sink.ignore)
//        .run()
//
//      Try(Await.result(jobReference, 1.second)) should matchPattern {
//        case Failure(_) =>
//      }
//    }
//
//    "succeed with JobReference even if downstream cancels" in {
//
//      val jobReference = Source
//        .single(PagingInfo(Some(JobInfo(JobReference(Some("jobId")), jobComplete = false)), None))
//        .viaMat(PaginatedRequest.JobReferenceMaterializer)(Keep.right)
//        .buffer(1, OverflowStrategy.backpressure)
//        .to(Sink.cancelled)
//        .run()
//
//      Await.result(jobReference, 1.second) should matchPattern {
//        case Some(JobReference(Some("jobId"))) =>
//      }
//    }
//
//  }

}
