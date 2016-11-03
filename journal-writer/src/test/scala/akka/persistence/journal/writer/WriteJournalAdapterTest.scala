/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.persistence.journal.writer

import akka.actor.{ ActorRef, Props }
import akka.persistence.JournalProtocol._
import akka.persistence.journal.writer.WriteJournalAdapterCameo._
import akka.persistence.query.{ EventEnvelope, EventEnvelope2, NoOffset }
import akka.persistence.{ AtomicWrite, PersistentImpl, PersistentRepr }
import akka.stream.alpakka.persistence.writer._
import akka.testkit.TestProbe

import scala.collection.immutable._
import scala.concurrent.duration._

class WriteJournalAdapterTest extends TestSpec {

  def validateAtomicWrite(aw: AtomicWrite, persistenceId: String): Unit = {
    aw.persistenceId shouldBe persistenceId
    aw.payload.size shouldBe 1
    aw.payload.head.persistenceId shouldBe persistenceId
  }

  it should "map a Seq[EventEnvelope] to a WriteMessages that has two persistenceIds" in {
    val write: WriteMessages = writeMessages(Seq(
      EventEnvelope(0, "pid1", 1L, "foo"),
      EventEnvelope(0, "pid2", 1L, "foo")
    ), null)
    write.messages.size shouldBe 2 // there are two atomic writes
    val aw1 = write.messages.map(_.asInstanceOf[AtomicWrite]).filter(_.persistenceId == "pid1").head
    validateAtomicWrite(aw1, "pid1")
    val aw2 = write.messages.map(_.asInstanceOf[AtomicWrite]).filter(_.persistenceId == "pid2").head
    validateAtomicWrite(aw2, "pid2")
  }

  def withJournal(f: TestProbe => TestProbe => ActorRef => Unit): Unit = {
    val sink = TestProbe()
    val journal = TestProbe()
    val adapter = system.actorOf(Props(new WriteJournalAdapter(journal.ref)))
    try f(sink)(journal)(adapter) finally killActors(adapter)
  }

  it should "send no instructions to write an empty Seq" in withJournal { sink => journal => adapter =>
    sink.send(adapter, Seq.empty[EventEnvelope])
    journal.expectNoMsg(100.millis)
    sink.expectMsg("ack")
  }

  it should "send instructions to write an EventEnvelope" in withJournal { sink => journal => adapter =>
    sink.send(adapter, EventEnvelope(0L, "pid1", 1L, "foo"))
    journal.expectMsgPF() { case WriteMessages(List(AtomicWrite(List(PersistentImpl("foo", 1L, "pid1", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")
  }

  it should "send instructions to write a Seq[EventEnvelope]" in withJournal { sink => journal => adapter =>
    sink.send(adapter, Seq(EventEnvelope(0L, "pid1", 1L, "foo")))
    journal.expectMsgPF() { case WriteMessages(Seq(AtomicWrite(Seq(PersistentRepr("foo", 1L)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")

    sink.send(adapter, Seq(EventEnvelope(0L, "pid1", 1L, "foo"), EventEnvelope(0L, "pid1", 2L, "bar")))
    journal.expectMsgPF() { case WriteMessages(Seq(AtomicWrite(Seq(PersistentImpl("foo", 1L, "pid1", _, _, _, _), PersistentImpl("bar", 2L, "pid1", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")
  }

  it should "send instructions to write an EventEnvelope2" in withJournal { sink => journal => adapter =>
    sink.send(adapter, EventEnvelope2(NoOffset, "pid2", 1L, "foo"))
    journal.expectMsgPF() { case WriteMessages(List(AtomicWrite(List(PersistentImpl("foo", 1L, "pid2", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")
  }

  it should "send instructions to write a Seq[EventEnvelope2]" in withJournal { sink => journal => adapter =>
    sink.send(adapter, Seq(EventEnvelope2(NoOffset, "pid2", 1L, "foo")))
    journal.expectMsgPF() { case WriteMessages(Seq(AtomicWrite(Seq(PersistentImpl("foo", 1L, "pid2", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")

    sink.send(adapter, Seq(EventEnvelope2(NoOffset, "pid2", 1L, "foo"), EventEnvelope2(NoOffset, "pid2", 2L, "bar")))
    journal.expectMsgPF() { case WriteMessages(Seq(AtomicWrite(Seq(PersistentImpl("foo", 1L, "pid2", _, _, _, _), PersistentImpl("bar", 2L, "pid2", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesSuccessful)
    sink.expectMsg("ack")
  }

  it should "fail on all other messages" in withJournal { sink => journal => adapter =>
    sink.send(adapter, "foo")
    journal.expectNoMsg(100.millis)
    sink.expectMsg(akka.actor.Status.Failure(unsupported))
  }

  it should "fail the sink when journal fails writing messages" in withJournal { sink => journal => adapter =>
    sink.send(adapter, EventEnvelope(0L, "pid1", 1L, "foo"))
    journal.expectMsgPF() { case WriteMessages(List(AtomicWrite(List(PersistentImpl("foo", 1L, "pid1", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessagesFailed(mockFailure))
    sink.expectMsg(akka.actor.Status.Failure(mockFailure))
  }

  it should "fail the sink when journal fails writing a message" in withJournal { sink => journal => adapter =>
    sink.send(adapter, EventEnvelope(0L, "pid1", 1L, "foo"))
    journal.expectMsgPF() { case WriteMessages(List(AtomicWrite(List(PersistentImpl("foo", 1L, "pid1", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessageFailure(null, mockFailure, 1))
    sink.expectMsg(akka.actor.Status.Failure(mockFailure))
  }

  it should "fail the sink when journal rejects a message" in withJournal { sink => journal => adapter =>
    sink.send(adapter, EventEnvelope(0L, "pid1", 1L, "foo"))
    journal.expectMsgPF() { case WriteMessages(List(AtomicWrite(List(PersistentImpl("foo", 1L, "pid1", _, _, _, _)))), _, _) => }
    journal.reply(WriteMessageRejected(null, mockFailure, 1))
    sink.expectMsg(akka.actor.Status.Failure(mockFailure))
  }
}
