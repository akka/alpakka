/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.CredentialsProvider
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings, HoverflySupport}
import akka.stream.scaladsl.{Keep, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.{created, serverError, success}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsObject, JsValue}

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
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("X-Upload-Content-Type", "application/octet-stream")
            .willReturn(success().header("Location", "https://example.com/upload123"))
            .put("/upload123")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes 0-9/10")
            .body("helloworld")
            .willReturn(serverError())
            .put("/upload123")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes */*")
            .willReturn(success().header("Range", "bytes=0-4"))
            .put("/upload123")
            .queryParam("prettyPrint", "false")
            .header("Authorization", "Bearer yyyy.c.an-access-token")
            .header("Content-Range", "bytes 5-9/10")
            .body("world")
            .willReturn(created().header("Content-Type", "application/json").body("{}"))
        )
      )

      val done = Source
        .single(ByteString("helloworld"))
        .toMat(LoadJob[JsValue](HttpRequest(POST, "https://example.com")))(Keep.right)
        .withAttributes(BigQueryAttributes.settings(settings))
        .run()

      Await.result(done, 10.seconds) shouldEqual JsObject.empty
    }

  }

}
