/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import akka.actor.{ ActorRef, Props }
import akka.persistence.query.{ EventEnvelope, EventEnvelope2, NoOffset }
import akka.stream.scaladsl.Source
import akka.testkit.TestProbe

class JournalWriterTest extends TestSpec {
  def withPid(pid: String = "pid1", pluginId: JournalIds => JournalIds = identity[JournalIds])(f: TestProbe => ActorRef => Unit): Unit = {
    val tp = TestProbe()
    val ref = system.actorOf(Props(new Messenger(pid, pluginId(JournalIds().InMemory).id)))
    try f(tp)(ref) finally killActors(ref)
  }

  it should "store events in store" in {
    withPid("pid1") { tp => ref =>
      tp.send(ref, "foo")
      tp.expectMsg("ack")
    }

    withPid("pid1") { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("foo - a")))
    }

    inMemoryReadJournal.currentEventsByPersistenceId("pid1", 0, Long.MaxValue).withTestProbe { tp =>
      tp.request(Long.MaxValue)
      tp.expectNext(EventEnvelope(1, "pid1", 1, MyMessage("foo - a")))
      tp.expectComplete()
    }

    withPid("pid1") { tp => ref =>
      tp.send(ref, "bar")
      tp.expectMsg("ack")
    }

    inMemoryReadJournal.currentEventsByPersistenceId("pid1", 0, Long.MaxValue).withTestProbe { tp =>
      tp.request(Long.MaxValue)
      tp.expectNext(EventEnvelope(1, "pid1", 1, MyMessage("foo - a")))
      tp.expectNext(EventEnvelope(2, "pid1", 2, MyMessage("bar - a")))
      tp.expectComplete()
    }
  }

  it should "write events into the event store using EventEnvelope" in {
    Source(List(
      EventEnvelope(0L, "pid1", 1, MyMessage("baz")),
      EventEnvelope(0L, "pid1", 2, MyMessage("quz"))
    )).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    withPid("pid1") { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - a"), MyMessage("quz - a")))
    }
  }

  it should "write events into the event store using Seq[EventEnvelope]" in {
    Source(List(
      EventEnvelope(0L, "pid1", 1, MyMessage("baz")),
      EventEnvelope(0L, "pid1", 2, MyMessage("quz"))
    )).grouped(2).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    withPid("pid1") { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - a"), MyMessage("quz - a")))
    }
  }

  it should "write events into the event store using EventEnvelope2" in {
    Source(List(
      EventEnvelope2(NoOffset, "pid1", 1, MyMessage("baz")),
      EventEnvelope2(NoOffset, "pid1", 2, MyMessage("quz"))
    )).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    withPid("pid1") { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - a"), MyMessage("quz - a")))
    }
  }

  it should "write events into the event store using Seq[EventEnvelope2]" in {
    Source(List(
      EventEnvelope2(NoOffset, "pid1", 1, MyMessage("baz")),
      EventEnvelope2(NoOffset, "pid1", 2, MyMessage("quz"))
    )).grouped(2).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    withPid("pid1") { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - a"), MyMessage("quz - a")))
    }
  }

  it should "write events to leveldb and read from leveldb" in {
    Source(List(
      EventEnvelope(0L, "foo1", 1, MyMessage("foo")),
      EventEnvelope(0L, "foo1", 2, MyMessage("bar")),
      EventEnvelope(0L, "foo2", 1, MyMessage("baz")),
      EventEnvelope(0L, "foo2", 2, MyMessage("qux")),
      EventEnvelope(0L, "foo2", 3, MyMessage("quz")),
      EventEnvelope(0L, "foo3", 1, MyMessage("zuul"))
    )).runWith(JournalWriter.sink(JournalIds().LevelDb.id)).toTry should be a 'success

    withPid("foo1", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("foo - b"), MyMessage("bar - b")))
    }

    withPid("foo2", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - b"), MyMessage("qux - b"), MyMessage("quz - b")))
    }

    withPid("foo3", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("zuul - b")))
    }
  }

  it should "read events from one store and write into another EventEnvelope" in {
    Source(List(
      EventEnvelope(0L, "pid1", 1, MyMessage("foo")),
      EventEnvelope(0L, "pid2", 1, MyMessage("bar")),
      EventEnvelope(0L, "pid3", 1, MyMessage("baz"))
    )).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    // read events from inMemory and write to LevelDb
    inMemoryReadJournal.currentPersistenceIds().flatMapConcat(pid =>
      inMemoryReadJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue)).runWith(JournalWriter.sink(JournalIds().LevelDb.id)).toTry should be a 'success

    withPid("pid1", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("foo - a - b")))
    }

    withPid("pid2", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("bar - a - b")))
    }

    withPid("pid3", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("baz - a - b")))
    }
  }

  it should "read events from one store and write into another grouped" in {
    Source(List(
      EventEnvelope(0L, "pid10", 1, MyMessage("foof")),
      EventEnvelope(0L, "pid10", 2, MyMessage("boof")),
      EventEnvelope(0L, "pid10", 3, MyMessage("doof")),
      EventEnvelope(0L, "pid11", 1, MyMessage("barf")),
      EventEnvelope(0L, "pid12", 1, MyMessage("bazf"))
    )).runWith(JournalWriter.sink(JournalIds().InMemory.id)).toTry should be a 'success

    // read events from inMemory and write to LevelDb
    inMemoryReadJournal.currentPersistenceIds().flatMapConcat(pid =>
      inMemoryReadJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue)).grouped(2).runWith(JournalWriter.sink(JournalIds().LevelDb.id)).toTry should be a 'success

    withPid("pid10", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("foof - a - b"), MyMessage("boof - a - b"), MyMessage("doof - a - b")))
    }

    withPid("pid11", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("barf - a - b")))
    }

    withPid("pid12", _.LevelDb) { tp => ref =>
      tp.send(ref, "state")
      tp.expectMsg(Seq(MyMessage("bazf - a - b")))
    }
  }

  it should "not send unknown types like eg. String" in {
    intercept[AssertionError] {
      Source.single("foo").runWith(JournalWriter.sink(JournalIds().LevelDb.id)).toTry should be a 'success
    }
  }
}
