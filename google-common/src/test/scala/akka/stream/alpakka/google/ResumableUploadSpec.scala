/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.{created, serverError, success}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.{JsObject, JsValue}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ResumableUploadSpec
    extends TestKit(ActorSystem("ResumableUploadSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestGoogleSettings
    with HoverflySupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ResumableUpload" should {

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
            .willReturn(serverError().header("Content-Type", "application/json").body("{}"))
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
        .via(ResumableUpload[JsValue](HttpRequest(POST, "https://example.com")))
        .toMat(Sink.last)(Keep.right)
        .withAttributes(GoogleAttributes.settings(settings))
        .run()

      Await.result(done, 10.seconds) shouldEqual JsObject.empty
    }

  }

}