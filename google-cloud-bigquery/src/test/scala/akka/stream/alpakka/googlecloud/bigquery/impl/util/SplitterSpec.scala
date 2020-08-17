/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class SplitterSpec
    extends TestKit(ActorSystem("BooleanSplitterSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()

  def createTestGraph[T](source: Source[Int, _], sink1: Sink[Int, T], sink2: Sink[Int, T]): RunnableGraph[(T, T)] =
    RunnableGraph.fromGraph(GraphDSL.create(sink1, sink2)((_, _)) { implicit builder => (s1, s2) =>
      import GraphDSL.Implicits._

      val splitter = builder.add(Splitter[Int](_ < 3)())

      source ~> splitter.in
      splitter.out(0) ~> s1
      splitter.out(1) ~> s2

      ClosedShape
    })

  "Splitter" must {

    "input splitting" in {
      val testGraph = createTestGraph(Source(0 until 10), Sink.seq, Sink.seq)

      val (trueResult, allResult) = testGraph.run()

      Await.result(trueResult, 1.second) shouldEqual (0 until 3)
      Await.result(allResult, 1.second) shouldEqual (3 until 10)
    }

    // TODO this test currently fails against Akka 2.5
    "backpressure when the 'false' side has no demand and a pending message" ignore {
      val testGraph = createTestGraph(Source(List(10, 2)), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      trueSink.request(1)

      // Even though the 'true' side has requested an element, the splitter backpressures
      // because the '10' is accepted on the 'false' side and there is no demand there:
      Try(trueSink.expectNext(20.milliseconds)).toOption shouldEqual None

      // Requesting the '10' makes the '2' flow through
      falseSink.request(1)
      trueSink.expectNext(2)

      // Of course as well as the '10' itself:
      falseSink.expectNext(10)
    }

    "backpressure when the 'true' side has no demand and a pending message" ignore {
      val testGraph = createTestGraph(Source(List(2, 10)), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      falseSink.request(1)

      // Even though the 'false' side has requested an element, the splitter backpressures
      // because the '2' is accepted on the 'false' side and there is no demand there:
      Try(falseSink.expectNext(20.milliseconds)).toOption shouldEqual None

      // Requesting the '2' makes the '10' flow through
      trueSink.request(1)
      falseSink.expectNext(10)

      // Of course as well as the '2' itself:
      trueSink.expectNext(2)
    }
  }
}
