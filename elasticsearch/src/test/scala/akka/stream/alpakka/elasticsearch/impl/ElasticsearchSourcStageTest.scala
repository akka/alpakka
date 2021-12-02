/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchSourcStageTest
    extends TestKit(ActorSystem("elasticsearchSourceStagetest"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with LogCapturing {

  implicit val mat: Materializer = Materializer(system)
  implicit val http: HttpExt = Http()

  "ElasticsearchSourceStage" when {
    "client cannot connect to ES" should {
      "stop the stream" in {
        val downstream = Source
          .fromGraph(
            new impl.ElasticsearchSourceStage[String](
              ElasticsearchParams.V7("es-simple-flow-index"),
              Map("query" -> """{ "match_all":{}}"""),
              ElasticsearchSourceSettings(ElasticsearchConnectionSettings("http://wololo:9202")),
              (json: String) => ScrollResponse(Some(json), None)
            )
          )
          .toMat(TestSink.probe)(Keep.right)
          .run()

        downstream.request(1)
        downstream.expectError()
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
