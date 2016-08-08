/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import org.reactivestreams.{ Publisher, Subscriber }

import scala.concurrent.duration.{ Duration, FiniteDuration }

class TimedSpecAutoFusingOn extends { val autoFusing = true } with TimedSpec
class TimedSpecAutoFusingOff extends { val autoFusing = false } with TimedSpec

trait TimedSpec extends BaseStreamSpec {

  import akka.stream.contrib.Implicits.TimedFlowDsl

  "Timed Source" should {

    "measure time it between elements matching a predicate" in {
      val testActor = TestProbe()

      val measureBetweenEvery = 5
      val n = 20

      val printInfo = (interval: Duration) ⇒ {
        testActor.ref ! interval
        info(s"Measured interval between $measureBetweenEvery elements was: $interval")
      }

      val flow = Flow[Int].map(identity).timedIntervalBetween(_ % measureBetweenEvery == 0, printInfo)

      val (source, sink) = Source(1 to n)
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
    }

    "measure time it takes from start to complete, by wrapping operations" in {
      val testActor = TestProbe()

      val n = 50
      val printInfo = (d: FiniteDuration) ⇒ {
        testActor.ref ! d
        info(s"Processing $n elements took $d")
      }

      val flow = Flow[Int].timed(_.map(identity), onComplete = printInfo)

      val (source, sink) = Source(1 to n)
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
    }

  }

  "Timed Flow" should {

    "measure time it between elements matching a predicate" in {
      val probe = TestProbe()

      val flow: Flow[Int, Long, _] = Flow[Int].map(_.toLong).timedIntervalBetween(in ⇒ in % 2 == 1, d ⇒ probe.ref ! d)

      val c1 = TestSubscriber.manualProbe[Long]()
      Source(List(1, 2, 3)).via(flow).runWith(Sink.fromSubscriber(c1))

      val s = c1.expectSubscription()
      s.request(100)
      c1.expectNext(1L)
      c1.expectNext(2L)
      c1.expectNext(3L)
      c1.expectComplete()

      val duration = probe.expectMsgType[Duration]
      info(s"Got duration (first): $duration")
    }

    "measure time from start to complete, by wrapping operations" in {
      val probe = TestProbe()

      // making sure the types come out as expected
      val flow: Flow[Int, String, _] =
        Flow[Int].
          timed(_.
            map(_.toDouble).
            map(_.toInt).
            map(_.toString), duration ⇒ probe.ref ! duration).
          map { s: String ⇒ s + "!" }

      val (flowIn: Subscriber[Int], flowOut: Publisher[String]) = flow.runWith(Source.asSubscriber[Int], Sink.asPublisher[String](false))

      val c1 = TestSubscriber.manualProbe[String]()
      val c2 = flowOut.subscribe(c1)

      val p = Source(0 to 100).runWith(Sink.asPublisher(false))
      p.subscribe(flowIn)

      val s = c1.expectSubscription()
      s.request(200)
      0 to 100 foreach { i ⇒ c1.expectNext(i.toString + "!") }
      c1.expectComplete()

      val duration = probe.expectMsgType[Duration]
      info(s"Took: $duration")
    }
  }
}
