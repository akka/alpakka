/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.{ CurrentEventsByPersistenceIdQuery, CurrentPersistenceIdsQuery, ReadJournal }
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestProbe
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers }

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

case class JournalIds(id: String = "") {
  def InMemory = this.copy(id = "inmemory-journal")
  def LevelDb = this.copy(id = "akka.persistence.journal.leveldb")
}

case class ReadJournalIds(id: String = "") {
  def InMemory = this.copy(id = "inmemory-read-journal")
  def LevelDb = this.copy(id = "akka.persistence.query.journal.leveldb")
}

abstract class TestSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll with BeforeAndAfterEach with LevelDbCleanup with InMemoryCleanup {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val pc: PatienceConfig = PatienceConfig(timeout = 3.seconds)
  implicit val timeout = Timeout(30.seconds)

  def readJournal(pluginId: ReadJournalIds => ReadJournalIds) = PersistenceQuery(system).readJournalFor(pluginId(ReadJournalIds()).id)
    .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery]

  val inMemoryReadJournal = readJournal(_.InMemory)
  val levelDbReadJournal = readJournal(_.LevelDb)

  val mockFailure: Throwable = new RuntimeException("Mock failure")

  def withEnvelopes[A](xs: Seq[A])(f: Source[A, _] => Unit): Unit =
    f(Source(xs))

  def killActors(actors: ActorRef*): Unit = {
    val tp = TestProbe()
    actors.foreach { (actor: ActorRef) =>
      tp watch actor
      actor ! PoisonPill
      tp.expectTerminated(actor)
    }
  }

  implicit class SourceOps[A](that: Source[A, _])(implicit system: ActorSystem) {
    def withTestProbe(f: TestSubscriber.Probe[A] => Unit): Unit =
      f(that.runWith(TestSink.probe(system)))
  }

  implicit class PimpedFuture[T](self: Future[T]) {
    def toTry: Try[T] = Try(self.futureValue)
  }

  override protected def afterAll(): Unit = {
    system.terminate().toTry should be a 'success
  }
}
