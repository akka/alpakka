/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.KillSwitches
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class RetrySpecAutoFusingOn extends { val autoFusing = true } with RetrySpec
class RetrySpecAutoFusingOff extends { val autoFusing = false } with RetrySpec

trait RetrySpec extends BaseStreamSpec {

  val failedElem: Try[Int] = Failure(new Exception("cooked failure"))
  def flow[T] = Flow[(Int, T)].map {
    case (i, j) if i % 2 == 0 => (failedElem, j)
    case (i, j)               => (Success(i + 1), j)
  }

  "Retry" should {
    "retry ints according to their parity" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .via(Retry(flow[Int]) { s =>
          if (s < 42) Some((s + 1, s + 1))
          else None
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(4))
      source.sendNext(42)
      assert(sink.expectNext()._1 === failedElem)
      source.sendComplete()
      sink.expectComplete()
    }

    "retry descending ints until success" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, (i to 0 by -2).toList ::: List(i + 1)))
        .via(Retry(flow[List[Int]]) {
          case x :: xs => Some(x -> xs)
          case Nil     => throw new IllegalStateException("should not happen")
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(4))
      source.sendNext(40)
      assert(sink.expectNext()._1 === Success(42))
      source.sendComplete()
      sink.expectComplete()
    }

    "retry squares by division" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, i * i))
        .via(Retry(flow[Int]) {
          case x if x % 4 == 0 => Some((x / 2, x / 4))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some((sqrt, x))
          }
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(4)
      assert(sink.expectNext()._1 === Success(2))
      sink.expectNoMsg(3.seconds)
      source.sendComplete()
      sink.expectComplete()
    }

    "tolerate killswitch terminations after start" in {
      val ((source, killSwitch), sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.both)
        .map(i => (i, i * i))
        .via(Retry(flow[Int]) {
          case x if x % 4 == 0 => Some((x / 2, x / 4))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some((sqrt, x))
          }
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations on start" in {
      val (killSwitch, sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.right)
        .map(i => (i, i))
        .via(Retry(flow[Int]) { x => Some((x, x + 1)) })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations before start" in {
      val (killSwitch, sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.right)
        .map(i => (i, i))
        .via(Retry(flow[Int]) { x => Some((x, x + 1)) })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      killSwitch.abort(failedElem.failed.get)
      sink.request(1)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow after start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val ((source, killSwitch), sink) = TestSource.probe[Int]
        .map(i => (i, i * i))
        .viaMat(Retry(innerFlow) {
          case x if x % 4 == 0 => Some((x / 2, x / 4))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some((sqrt, x))
          }
        })(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow on start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val (killSwitch, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .viaMat(Retry(innerFlow) { x => Some((x, x + 1)) })(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow before start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val (killSwitch, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .viaMat(Retry(innerFlow) { x => Some((x, x + 1)) })(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      killSwitch.abort(failedElem.failed.get)
      sink.request(1)
      sink.expectError(failedElem.failed.get)
    }
  }

  "RetryConcat" should {
    "swallow failed elements that are retried with an empty seq" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .via(Retry.concat(100, flow[Int]) { _ => Some(Nil) })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      sink.expectNoMsg()
      source.sendNext(3)
      assert(sink.expectNext()._1 === Success(4))
      source.sendNext(4)
      sink.expectNoMsg()
      source.sendComplete()
      sink.expectComplete()
    }

    "concat incrementd ints and modulo 3 incremented ints from retries" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .via(Retry.concat(100, flow[Int]) { os =>
          val s = (os + 1) % 3
          if (os < 42) Some(List((os + 1, os + 1), (s, s)))
          else if (os == 42) Some(Nil)
          else None
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(4))
      assert(sink.expectNext()._1 === Success(2))
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(44)
      assert(sink.expectNext()._1 === failedElem)
      source.sendNext(42)
      sink.expectNoMsg()
      source.sendComplete()
      sink.expectComplete()
    }

    "retry squares by division" in {
      val (source, sink) = TestSource.probe[Int]
        .map(i => (i, i * i))
        .via(Retry.concat(100, flow[Int]) {
          case x if x % 4 == 0 => Some(List((x / 2, x / 4)))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some(List((sqrt, x)))
          }
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(4)
      assert(sink.expectNext()._1 === Success(2))
      sink.expectNoMsg(3.seconds)
      source.sendComplete()
      sink.expectComplete()
    }

    "tolerate killswitch terminations after start" in {
      val ((source, killSwitch), sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.both)
        .map(i => (i, i * i))
        .via(Retry.concat(100, flow[Int]) {
          case x if x % 4 == 0 => Some(List((x / 2, x / 4)))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some(List((sqrt, x)))
          }
        })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations on start" in {
      val (killSwitch, sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.right)
        .map(i => (i, i))
        .via(Retry.concat(100, flow[Int]) { x => Some(List((x, x + 1))) })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations before start" in {
      val (killSwitch, sink) = TestSource.probe[Int]
        .viaMat(KillSwitches.single[Int])(Keep.right)
        .map(i => (i, i))
        .via(Retry.concat(100, flow[Int]) { x => Some(List((x, x + 1))) })
        .toMat(TestSink.probe)(Keep.both)
        .run()

      killSwitch.abort(failedElem.failed.get)
      sink.request(1)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow after start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val ((source, killSwitch), sink) = TestSource.probe[Int]
        .map(i => (i, i * i))
        .viaMat(Retry.concat(100, innerFlow) {
          case x if x % 4 == 0 => Some(List((x / 2, x / 4)))
          case x => {
            val sqrt = scala.math.sqrt(x.toDouble).toInt
            Some(List((sqrt, x)))
          }
        })(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      source.sendNext(1)
      assert(sink.expectNext()._1 === Success(2))
      source.sendNext(2)
      assert(sink.expectNext()._1 === Success(2))
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow on start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val (killSwitch, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .viaMat(Retry.concat(100, innerFlow) { x => Some(List((x, x + 1))) })(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)
      killSwitch.abort(failedElem.failed.get)
      sink.expectError(failedElem.failed.get)
    }

    "tolerate killswitch terminations inside the flow before start" in {
      val innerFlow = flow[Int].viaMat(KillSwitches.single[(Try[Int], Int)])(Keep.right)
      val (killSwitch, sink) = TestSource.probe[Int]
        .map(i => (i, i))
        .viaMat(Retry.concat(100, innerFlow) { x => Some(List((x, x + 1))) })(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      killSwitch.abort(failedElem.failed.get)
      sink.request(1)
      sink.expectError(failedElem.failed.get)
    }
  }
}
