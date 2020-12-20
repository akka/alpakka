/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.stream.alpakka.googlecloud.bigquery.BigQueryException
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class AddPageTokenSpec
    extends TestKit(ActorSystem("AddPageTokenSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val getRequest = HttpRequest(GET, "/123")
  val postRequest = HttpRequest(POST).withEntity("request body")
  val requests = List(getRequest, postRequest)
  val jobInfos = List(false, true).map(jobComplete => JobInfo(JobReference(None, Some("123"), None), jobComplete))
  val optionalJobInfos = None +: jobInfos.map(Some(_))
  val optionalPageToken = List(None, Some("thePage"))
  val pagingInfos =
    for (jobInfo <- optionalJobInfos; pageToken <- optionalPageToken) yield PagingInfo(jobInfo, pageToken)
  val optionalPagingInfos = None +: pagingInfos.map(Some(_))
  val inputs = for (request <- requests; pagingInfo <- optionalPagingInfos) yield (request, pagingInfo)

  val getRequestWithPageToken = getRequest.withUri(getRequest.uri.withQuery(Query("pageToken" -> "thePage")))
  val bigQueryException = BigQueryException(
    "Cannot get the next page of a query request without a JobId in the response."
  )
  val expectedOutputs = List(
    Success(getRequest),
    Success(getRequest),
    Success(getRequestWithPageToken),
    Success(getRequest),
    Success(getRequest),
    Success(getRequest),
    Success(getRequestWithPageToken),
    Success(postRequest),
    Failure(bigQueryException),
    Failure(bigQueryException),
    Success(getRequest),
    Success(getRequest),
    Success(getRequest),
    Success(getRequestWithPageToken)
  )

  private def humanReadable(request: HttpRequest): String = request match {
    case `getRequest` => "a GET request"
    case `postRequest` => "a POST request"
    case `getRequestWithPageToken` => "a GET request with a page token"
  }

  private def humanReadable(pagingInfo: Option[PagingInfo]): String = pagingInfo match {
    case None => "no paging info"
    case Some(PagingInfo(jobInfo, pageToken)) =>
      val readableJob = jobInfo.fold("not a job")(j => s"job is ${if (j.jobComplete) "complete" else "incomplete"}")
      val readablePageToken = pageToken.fold("has no page token")(_ => "has page token")
      s"$readableJob and $readablePageToken"
  }

  private def addPageToken(input: (HttpRequest, Option[PagingInfo])): Try[HttpRequest] = {
    val result = Source.single(input).via(AddPageToken()).runWith(Sink.head[HttpRequest])
    Try(Await.result(result, 1.second))
  }

  "AddPageToken" must {

    inputs.zip(expectedOutputs).foreach {
      case (input, expectedOutput) =>
        val testName = s"make ${humanReadable(input._1)} into ${expectedOutput.toOption
          .fold("IllegalStateException")(humanReadable)} when ${humanReadable(input._2)}"
        testName in {
          val result = addPageToken(input)
          result match {
            case Success(_) => result shouldEqual expectedOutput
            case Failure(_) => result.toString shouldEqual expectedOutput.toString
          }
        }
    }
  }

}
