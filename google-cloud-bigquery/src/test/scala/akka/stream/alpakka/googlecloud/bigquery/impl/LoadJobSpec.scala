/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings, HoverflySupport}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.SprayJsonSupport._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.{created, serverError, success}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.JsValue

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class LoadJobSpec
    extends TestKit(ActorSystem("LoadJobSpec"))
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

  "LoadJobs" should {

    "complete interrupted upload" in {

      hoverfly.simulate(
        dsl(
          service("example.com")
            .post("/")
            .queryParam("uploadType", "resumable")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("X-Upload-Content-Type", "application/octet-stream")
            .willReturn(success().header("Location", "https://example.com/upload123"))
            .put("/upload123")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes 0-9/10")
            .body("helloworld")
            .willReturn(serverError())
            .put("/upload123")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes */*")
            .willReturn(success().header("Range", "bytes=0-4"))
            .put("/upload123")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes 5-9/10")
            .body("world")
            .willReturn(created().header("Content-Type", "application/json").body("{}"))
        )
      )

      val done = Source
        .single(ByteString("helloworld"))
        .via(LoadJob[JsValue](HttpRequest(POST, "https://example.com")))
        .withAttributes(BigQueryAttributes.settings(settings))
        .runWith(Sink.ignore)

      Await.result(done, 10.seconds) shouldEqual Done
    }

  }

}
