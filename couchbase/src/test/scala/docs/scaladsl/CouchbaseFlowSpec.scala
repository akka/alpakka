/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument, StringDocument}
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterEach, BeforeAfterAll}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CouchbaseFlowSpec extends Specification with BeforeAfterAll with AfterEach with CouchbaseSupport with Matchers {

  sequential

  "Couchbase Sink" should {

    "insert single object as RawJsonDocument" in {
      val result: Future[Done] =
        Source.single(head).map(createCBRawJson).via(CouchbaseFlow.upsertSingle(1, akkaBucket)).runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[RawJsonDocument] =
        CouchbaseSource[RawJsonDocument](head.id, akkaBucket).runWith(Sink.head[RawJsonDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert single object as JsonDocument" in {
      val result: Future[Done] =
        Source.single(head).map(createCBJson).via(CouchbaseFlow.upsertSingle(1, akkaBucket)).runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[JsonDocument] =
        CouchbaseSource[JsonDocument](head.id, akkaBucket).runWith(Sink.head[JsonDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert single object as StringDocument" in {
      val result: Future[Done] =
        Source.single(head).map(createCBString).via(CouchbaseFlow.upsertSingle(1, akkaBucket)).runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[StringDocument] =
        CouchbaseSource[StringDocument](head.id, akkaBucket).runWith(Sink.head[StringDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert single object as BinaryDocument" in {
      val result: Future[Done] =
        Source.single(head).map(createCBBinary).via(CouchbaseFlow.upsertSingle(1, akkaBucket)).runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[BinaryDocument] =
        CouchbaseSource[BinaryDocument](head.id, akkaBucket).runWith(Sink.head[BinaryDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert multipleObjects as RawJsonDocument" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[RawJsonDocument]] =
        CouchbaseSource[RawJsonDocument](bulk.map(_.id), akkaBucket).runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as JsonDocument" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[JsonDocument]] =
        CouchbaseSource[JsonDocument](bulk.map(_.id), akkaBucket).runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as StringDocument" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBString)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[StringDocument]] =
        CouchbaseSource[StringDocument](bulk.map(_.id), akkaBucket).runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as BinaryDocument" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBBinary)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[BinaryDocument]] =
        CouchbaseSource[BinaryDocument](bulk.map(_.id), akkaBucket).runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "delete single element" in {
      val upsertFuture: Future[Done] =
        Source.single(head).map(createCBRawJson).via(CouchbaseFlow.upsertSingle(1, akkaBucket)).runWith(Sink.ignore)
      //wait til operation completed
      Await.result(upsertFuture, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] =
        Source.single(head.id).via(CouchbaseFlow.deleteOne(1, akkaBucket)).runWith(Sink.ignore)
      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[RawJsonDocument] = CouchbaseSource(head.id, akkaBucket).runWith(Sink.head[RawJsonDocument])
      Await.result(getFuture, Duration(5, TimeUnit.SECONDS)) must throwA[NoSuchElementException]
    }

    "delete bulk of elements" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] = Source
        .fromIterator(() => bulk.map(_.id).iterator)
        .grouped(2)
        .via(CouchbaseFlow.deleteBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[Seq[RawJsonDocument]] =
        CouchbaseSource(bulk.map(_.id), akkaBucket).runWith(Sink.seq[RawJsonDocument])
      val getResult: Seq[RawJsonDocument] = Await.result(getFuture, Duration(5, TimeUnit.SECONDS))
      getResult.length shouldEqual 0
    }

    "delete bulk of elements and not all exists" in {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(CouchbaseFlow.upsertBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] = Source
        .fromIterator(() => ("NoneExisting" +: bulk.map(_.id)).iterator)
        .grouped(2)
        .via(CouchbaseFlow.deleteBulk(2, akkaBucket))
        .runWith(Sink.ignore)
      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[Seq[RawJsonDocument]] =
        CouchbaseSource(bulk.map(_.id), akkaBucket).runWith(Sink.seq[RawJsonDocument])
      val getResult: Seq[RawJsonDocument] = Await.result(getFuture, Duration(5, TimeUnit.SECONDS))

      getResult.length shouldEqual 0
    }
  }

  override protected def after: Any = cleanAllInBucket(bulk.map(_.id), akkaBucket)
  // override def beforeAll(): Unit = ()
}
