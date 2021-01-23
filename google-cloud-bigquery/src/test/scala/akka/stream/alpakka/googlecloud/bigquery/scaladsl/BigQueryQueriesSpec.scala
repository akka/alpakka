package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import _root_.spray.json._
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonFormat
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQueryEndpoints, BigQuerySettings, HoverflySupport}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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

  implicit val settings = BigQuerySettings()
    .copy(credentialsProvider = new CredentialsProvider {
      override def projectId: String = ???
      override def getToken()(implicit ec: ExecutionContext, settings: BigQuerySettings): Future[OAuth2BearerToken] =
        Future.successful(OAuth2BearerToken("yyyy.c.an-access-token"))
    })

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

  "BigQueryQueries" should {

    "get query results" when {

      implicit object jsValueFormat extends BigQueryRootJsonFormat[JsValue] {
        override def write(obj: JsValue): JsValue = obj
        override def read(json: JsValue): JsValue = json
      }

      "succeeds immediately and has one page" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .anyBody()
              .willReturn(success(completeQuery.toJson.toString(), "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(BigQueryAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage")))
      }

      "succeeds immediately and has two pages" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .anyBody()
              .willReturn(success(completeQueryWith2ndPage.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("pageToken", pageToken)
              .willReturn(success(query2ndPage.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(BigQueryAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage"), JsString("secondPage")))
      }

      "succeeds on 2nd attempt and has one page" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .anyBody()
              .willReturn(success(incompleteQuery.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .willReturn(success(completeQuery.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(BigQueryAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage")))
      }

      "succeeds on 2nd attempt and has two pages" in {

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .anyBody()
              .willReturn(success(incompleteQuery.toJson.toString(), "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .willReturn(success(completeQueryWith2ndPage.toJson.toString, "application/json"))
              .get(BigQueryEndpoints.query(settings.projectId, jobId).path.toString)
              .queryParam("pageToken", pageToken)
              .willReturn(success(query2ndPage.toJson.toString, "application/json"))
          )
        )

        query[JsValue]("SQL")
          .addAttributes(BigQueryAttributes.settings(settings))
          .runWith(Sink.seq[JsValue])
          .map(_ shouldEqual Seq(JsString("firstPage"), JsString("secondPage")))
      }

    }

    "fail" when {

      "parser is broken" in {

        implicit object brokenFormat extends BigQueryRootJsonFormat[JsValue] {
          override def write(obj: JsValue): JsValue = obj
          override def read(json: JsValue): JsValue = throw new RuntimeException
        }

        hoverfly.reset()
        hoverfly.simulate(
          dsl(
            service(BigQueryEndpoints.queries(settings.projectId).authority.host.address())
              .post(BigQueryEndpoints.queries(settings.projectId).path.toString)
              .anyBody()
              .willReturn(success(completeQuery.toJson.toString(), "application/json"))
          )
        )

        recoverToSucceededIf[RuntimeException] {
          query[JsValue]("SQL")
            .addAttributes(BigQueryAttributes.settings(settings))
            .runWith(Sink.seq[JsValue])
        }
      }
    }

  }

}
