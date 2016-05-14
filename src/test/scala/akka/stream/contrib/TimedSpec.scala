/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.{ Flow, Keep }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }

import scala.concurrent.duration.Duration

class TimedSpecAutoFusingOn extends { val autoFusing = true } with TimedSpec
class TimedSpecAutoFusingOff extends { val autoFusing = false } with TimedSpec

trait TimedSpec extends BaseStreamSpec {

  "Timed Source" should {

    import akka.stream.contrib.Implicits.TimedFlowDsl

    "measure time it between elements matching a predicate" in {

      val measureBetweenEvery = 5
      val printInfo = (interval: Duration) ⇒ {
        info(s"Measured interval between $measureBetweenEvery elements was: $interval")
      }

      val flow = Flow[Int].map(identity).timedIntervalBetween(_ % measureBetweenEvery == 0, printInfo)
      val (source, sink) = TestSource.probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      /*sink.request(99)
      source.sendNext(1)
      source.sendComplete()
      sink.expectComplete()
      source.sendNext(2)
      source.sendNext(3)
      sink.expectNext(1, 3, 6)
      source.sendComplete()
      sink.expectComplete()*/
      /*

      val n = 20
      val testRuns = 1 to 2

      def script = Script((1 to n) map { x ⇒ Seq(x) → Seq(x) }: _*)
      testRuns foreach (_ ⇒ runScript(script, settings) { flow ⇒
        flow.
          map(identity).
          timedIntervalBetween(_ % measureBetweenEvery == 0, onInterval = printInfo)
      })

      val expectedNrOfOnIntervalCalls = testRuns.size * ((n / measureBetweenEvery) - 1) // first time has no value to compare to, so skips calling onInterval
      1 to expectedNrOfOnIntervalCalls foreach { _ ⇒ testActor.expectMsgType[Duration] }*/
    }

    "measure time it takes from start to complete, by wrapping operations" in {

    }

  }

  "Timed Flow" should {

    "measure time it between elements matching a predicate" in {

    }

    "measure time from start to complete, by wrapping operations" in {

    }
  }
}
