/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.impl

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Random

class LogEntryQueueSpec
    extends TestKit(ActorSystem("LogEntryQueueSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val events = {
    val now = Instant.now()
    Vector.tabulate(9, 10) { (i, j) =>
      val severity = LogSeverity(i * 100)
      val timestamp = now.plusSeconds(j)
      LogEntry(None, None, Some(timestamp), Some(severity), None, Map.empty, ())
    }
  }

  "LogEntryQueue" should {

    "keep every event if there is no backpressure" in {

      val flow = LogEntryQueue[Unit](1, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropHead)

      val src = Random.shuffle(events(1) ++ events(2) ++ events(3))
      val snk = Source(src).via(flow).mapConcat(identity).runWith(Sink.seq)
      snk.futureValue should contain theSameElementsAs src
    }

    "drop oldest low-priority event" in {

      val flow = LogEntryQueue[Unit](2, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropHead)

      val probe = Source(List(events(3)(0), events(4)(1), events(3)(2)))
        .via(flow)
        .runWith(TestSink.probe)

      probe.requestNext() should contain theSameElementsAs List(events(3)(2), events(4)(1))
    }

    "drop youngest low-priority event" in {

      val flow = LogEntryQueue[Unit](2, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropTail)

      val probe = Source(List(events(3)(0), events(4)(1), events(3)(2)))
        .via(flow)
        .runWith(TestSink.probe)

      probe.requestNext() should contain theSameElementsAs List(events(3)(0), events(4)(1))
    }

    "not emit until queue is full" in {

      val flow = LogEntryQueue[Unit](10, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropTail)

      val evs = Random.shuffle(events(1) ++ events(2) ++ events(3))
      val (src, snk) = flow.runWith(TestSource.probe[LogEntry[Unit]], TestSink.probe)

      snk.request(1)
      evs.take(9).foreach { e =>
        src.sendNext(e)
        snk.expectNoMessage()
      }
      src.sendNext(evs(9))
      snk.expectNext() should contain theSameElementsAs evs.take(10)
    }

    "emit early if a high-severity event is queued" in {

      val flow = LogEntryQueue[Unit](10, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropTail)

      val evs = List(events(3)(0), events(4)(1), events(3)(2), events(8)(3), events(2)(4))
      val (src, snk) = flow.runWith(TestSource.probe[LogEntry[Unit]], TestSink.probe)

      snk.request(1)
      evs.take(3).foreach { e =>
        src.sendNext(e)
        snk.expectNoMessage()
      }
      src.sendNext(evs(3))
      src.sendNext(evs(4))
      snk.expectNext() should contain theSameElementsAs evs.dropRight(1)
    }

    "emit within `flushWithin`" in {

      val flow = LogEntryQueue[Unit](10, LogSeverity.Emergency, 1.second, OverflowStrategy.dropTail)

      val evs = List(events(3)(0), events(4)(1), events(3)(2), events(2)(3))
      val (src, snk) = flow.runWith(TestSource.probe[LogEntry[Unit]], TestSink.probe)

      snk.request(1)
      evs.foreach { e =>
        src.sendNext(e)
        snk.expectNoMessage()
      }
      snk.expectNext(1.second) should contain theSameElementsAs evs
    }

    "emit as soon as downstream becomes available" in {

      val flow = LogEntryQueue[Unit](10, LogSeverity.Emergency, 1.hour, OverflowStrategy.dropTail)

      val evs = List(events(3)(0), events(4)(1), events(3)(2), events(8)(3), events(2)(4), events(5)(5))
      val (src, snk) = flow.runWith(TestSource.probe[LogEntry[Unit]], TestSink.probe)

      evs.take(5).foreach { e =>
        src.sendNext(e)
      }
      snk.request(1)
      src.sendNext(evs(5))
      snk.expectNext() should contain theSameElementsAs evs.dropRight(1)
    }

  }

}
