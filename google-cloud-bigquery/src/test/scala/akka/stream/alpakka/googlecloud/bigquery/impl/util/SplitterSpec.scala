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

    "backpressure only if all outputs are pulled - if only requested from true" in {
      val testGraph = createTestGraph(Source.single(2), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      trueSink.request(1)

      Try(trueSink.expectNext(20.milliseconds)).toOption shouldEqual None

      falseSink.request(1)

      trueSink.expectNext(2)

    }

    "backpressure only if all outputs are pulled - if only requested from false" in {
      val testGraph = createTestGraph(Source.single(4), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      falseSink.request(1)

      Try(falseSink.expectNext(20.milliseconds)).toOption shouldEqual None

      trueSink.request(1)

      falseSink.expectNext(4)

    }

  }
}
