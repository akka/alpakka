/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch.{StringMessageWriter, _}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchSimpleFlowStageTest
    extends TestKit(ActorSystem("elasticsearchtest"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with LogCapturing {

  implicit val mat: Materializer = Materializer(system)
  implicit val http: HttpExt = Http()

  val writer: StringMessageWriter = StringMessageWriter.getInstance
  val settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings(
    ElasticsearchConnectionSettings("http://localhost:9202")
  )
  val dummyMessages: (immutable.Seq[WriteMessage[String, NotUsed]], immutable.Seq[WriteResult[String, NotUsed]]) = (
    immutable.Seq(
      WriteMessage.createIndexMessage("1", """{"foo": "bar"}"""),
      WriteMessage.createIndexMessage("2", """{"foo2": "bar2"}"""),
      WriteMessage.createIndexMessage("3", """{"foo3": "bar3"}""")
    ),
    immutable.Seq[WriteResult[String, NotUsed]]()
  )

  "ElasticsearchSimpleFlowStage" when {
    "stream ends" should {
      "emit element only when downstream requests" in {
        val (upstream, downstream) =
          TestSource
            .probe[(immutable.Seq[WriteMessage[String, NotUsed]], immutable.Seq[WriteResult[String, NotUsed]])]
            .via(
              new impl.ElasticsearchSimpleFlowStage[String, NotUsed](
                ElasticsearchParams.V7("es-simple-flow-index"),
                settings,
                writer
              )
            )
            .toMat(TestSink.probe)(Keep.both)
            .run()

        upstream.sendNext(dummyMessages)
        upstream.sendNext(dummyMessages)
        upstream.sendNext(dummyMessages)
        upstream.sendComplete()

        downstream.request(2)
        downstream.expectNextN(2)
        downstream.request(1)
        downstream.expectNextN(1)
        downstream.expectComplete()
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
