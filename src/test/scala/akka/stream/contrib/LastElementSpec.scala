/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestDuration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Await, Future }

class LastElementSpecAutoFusingOn extends { val autoFusing = true } with LastElementSpec
class LastElementSpecAutoFusingOff extends { val autoFusing = false } with LastElementSpec

trait LastElementSpec extends BaseStreamSpec {

  "A stream via LastElement" should {
    "materialize to the last element emitted by a finite nonempty successful source" in {
      val (lastElement, probe) = Source(Vector(1, 2, 3))
        .viaMat(LastElement())(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      probe
        .request(3)
        .expectNext(1, 2, 3)
        .expectComplete()
      Await.result(lastElement, 1.second.dilated) shouldBe Some(3)
    }

    "materialize to `None` for an empty successful source" in {
      val (lastElement, probe) = Source(Vector.empty[Int])
        .viaMat(LastElement())(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      probe
        .request(3)
        .expectComplete()
      Await.result(lastElement, 1.second.dilated) shouldBe None
    }

    "materialize to the last element emitted by a source before it failed" in {
      import system.dispatcher
      val (lastElement, lastEmitted) = Source.fromIterator(() => Iterator.iterate(1)(n => if (n >= 3) sys.error("FAILURE") else n + 1))
        .viaMat(LastElement())(Keep.right)
        .toMat(Sink.fold[Option[Int], Int](None)((_, o) => Some(o)))(Keep.both)
        .run()
      val Vector(l1, l2) = Await.result(Future.sequence(Vector(lastElement, lastEmitted)), 1.second.dilated)
      l1 shouldBe l2
    }
  }
}
