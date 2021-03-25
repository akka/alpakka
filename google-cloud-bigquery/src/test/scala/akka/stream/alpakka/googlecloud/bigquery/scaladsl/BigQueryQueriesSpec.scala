/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import _root_.spray.json._
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.alpakka.google.auth.NoCredentials
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.stream.alpakka.googlecloud.bigquery.model.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryEndpoints, HoverflySupport}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class BigQueryQueriesSpec
    extends TestKit(ActorSystem("BigQueryQueriesSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport
    with BigQueryRest
    with BigQueryQueries {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit def queryResponseFormat[T: JsonFormat]: RootJsonFormat[QueryResponse[T]] = {
    import DefaultJsonProtocol._
    jsonFormat10(QueryResponse[T])
  }

  implicit val settings = GoogleSettings().copy(credentials = NoCredentials("", ""))

  val jobId = "jobId"
  val pageToken = "pageToken"

  val incompleteQuery = QueryResponse[JsValue](
    None,
    JobReference(Some(settings.projectId), Some(jobId), None),
    None,
    None,
    None,
    None,
    false,
    None,
    None,
    None
  )

  val completeQuery = incompleteQuery.copy[JsValue](
    jobComplete = true,
    rows = Some(JsString("firstPage") :: Nil)
  )

  val completeQueryWith2ndPage = completeQuery.copy[JsValue](
    pageToken = Some(pageToken)
  )

  val query2ndPage = completeQuery.copy[JsValue](
    rows = Some(JsString("secondPage") :: Nil)
  )

  val completeQueryWithoutJobId = completeQuery.copy[JsValue](
    jobReference = JobReference(None, None, None)
  )

  "BigQueryQueries" should {

    "get query results" when {
      import DefaultJsonProtocol._

      "completes immediately and has one page" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(completeQuery.toJson.toString(), "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(GoogleAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage")))
      }

      "completes immediately and has two pages" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(completeQueryWith2ndPage.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("pageToken", pageToken)
              .queryParam("prettyPrint", "false")
              .willReturn(success(query2ndPage.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(GoogleAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage"), JsString("secondPage")))
      }

      "completes on 2nd attempt and has one page" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(incompleteQuery.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("prettyPrint", "false")
              .willReturn(success(completeQuery.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(GoogleAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage")))
      }

      "completes on 2nd attempt and has two pages" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(incompleteQuery.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("prettyPrint", "false")
              .willReturn(success(completeQueryWith2ndPage.toJson.toString, "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("pageToken", pageToken)
              .queryParam("prettyPrint", "false")
              .willReturn(success(query2ndPage.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(GoogleAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage"), JsString("secondPage")))
      }

      "completes immediately without job id" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(completeQueryWithoutJobId.toJson.toString(), "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(GoogleAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage")))
      }

    }

    "fail" when {

      "parser is broken" in {

        class BrokenParserException extends Exception

        implicit object brokenFormat extends JsonFormat[JsValue] {
          override def write(obj: JsValue): JsValue = obj
          override def read(json: JsValue): JsValue = throw new BrokenParserException
        }

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .queryParam("prettyPrint", "false")
              .anyBody()
              .willReturn(success(completeQuery.toJson.toString(), "application/json"))
          )
        )

        recoverToSucceededIf[BrokenParserException] {
          query[JsValue]("SQL")
            .addAttributes(GoogleAttributes.settings(settings))
            .runWith(Sink.seq[JsValue])
        }
      }
    }

  }

}
