/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.alpakka.couchbase.{BulkOperationResult, FailedOperation, SingleOperationResult}
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Sink, Source}

//#write-settings
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import com.couchbase.client.java.{PersistTo, ReplicateTo}
//#write-settings

import com.couchbase.client.java.document.{BinaryDocument, RawJsonDocument, StringDocument}
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterEach, BeforeAfterAll}
import scala.collection.immutable.Seq
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import akka.stream.testkit.scaladsl.StreamTestKit._

//#init-sourceBulk
import com.couchbase.client.java.document.JsonDocument

//#init-sourceBulk

class CouchbaseFlowSpec extends Specification with BeforeAfterAll with AfterEach with CouchbaseSupport with Matchers {

  sequential

  "Couchbase Flow" should {

    "create default CouchbaseWriteSettings object" in assertAllStagesStopped {

      val couchbaseWriteSettings = CouchbaseWriteSettings()
      val expectedCouchbaseWriteSettings = CouchbaseWriteSettings(1, ReplicateTo.ONE, PersistTo.NONE, 2.seconds)
      couchbaseWriteSettings shouldEqual expectedCouchbaseWriteSettings
    }

    "create custom CouchbaseWriteSettings object" in {

      //#write-settings
      val couchbaseWriteSettings = CouchbaseWriteSettings()
        .withParallelism(3)
        .withPersistTo(PersistTo.FOUR)
        .withReplicateTo(ReplicateTo.THREE)
        .withTimeout(2.seconds)
      //#write-settings

      val expectedCouchbaseWriteSettings = CouchbaseWriteSettings(3, ReplicateTo.THREE, PersistTo.FOUR, 2.seconds)
      couchbaseWriteSettings shouldEqual expectedCouchbaseWriteSettings
    }

    "insert single object as RawJsonDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(head)
          .map(createCBRawJson)
          .via(CouchbaseFlow.upsertSingle(couchbaseWriteSettings, akkaBucket))
          .runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[RawJsonDocument] =
        CouchbaseSource
          .fromSingleId[RawJsonDocument](head.id, akkaBucket, classOf[RawJsonDocument])
          .runWith(Sink.head[RawJsonDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id

    }

    "insert single object as JsonDocument" in assertAllStagesStopped {

      // #by-single-id-flow
      import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
      import akka.Done
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource

      val obj = TestObject("First", "First")

      val result: Future[Done] =
        Source
          .single(obj)
          .map(createCBJson)
          .via(CouchbaseFlow.upsertSingle(couchbaseWriteSettings, akkaBucket))
          .runWith(Sink.ignore)

      // #by-single-id-flow
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      //#init-sourceSingle
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource
      import com.couchbase.client.java.document.JsonDocument

      val id: String = "First"

      //#init-sourceSingle
      val msgFuture: Future[JsonDocument] =
        //#init-sourceSingle
        CouchbaseSource
          .fromSingleId[JsonDocument](id, akkaBucket, classOf[JsonDocument])
          .runWith(Sink.head[JsonDocument])
      //#init-sourceSingle
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert single object as StringDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(head)
          .map(createCBString)
          .via(CouchbaseFlow.upsertSingle(couchbaseWriteSettings, akkaBucket))
          .runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[StringDocument] =
        CouchbaseSource
          .fromSingleId[StringDocument](head.id, akkaBucket, classOf[StringDocument])
          .runWith(Sink.head[StringDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert single object as BinaryDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(head)
          .map(createCBBinary)
          .via(CouchbaseFlow.upsertSingle(couchbaseWriteSettings, akkaBucket))
          .runWith(Sink.ignore)
      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val msgFuture: Future[BinaryDocument] =
        CouchbaseSource
          .fromSingleId[BinaryDocument](head.id, akkaBucket, classOf[BinaryDocument])
          .runWith(Sink.head[BinaryDocument])
      Await.result(msgFuture, Duration(5, TimeUnit.SECONDS)).id() shouldEqual head.id
    }

    "insert multipleObjects as RawJsonDocument" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[RawJsonDocument]] =
        CouchbaseSource
          .fromBulkIds[RawJsonDocument](bulk.map(_.id), akkaBucket, classOf[RawJsonDocument])
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as JsonDocument" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings()
                                     .withParallelism(2)
                                     .withPersistTo(PersistTo.NONE)
                                     .withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      //#init-sourceBulk
      val ids: Seq[String] = Seq("First", "Second", "Third", "Fourth")

      val futureResult: Future[immutable.Seq[JsonDocument]] =
        CouchbaseSource.fromBulkIds[JsonDocument](ids, akkaBucket, classOf[JsonDocument]).runWith(Sink.seq)
      //#init-sourceBulk

      val result = Await.result(futureResult, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as StringDocument" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBString)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[StringDocument]] =
        CouchbaseSource
          .fromBulkIds[StringDocument](bulk.map(_.id), akkaBucket, classOf[StringDocument])
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multipleObjects as BinaryDocument" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBBinary)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val resultsAsFuture: Future[immutable.Seq[BinaryDocument]] =
        CouchbaseSource
          .fromBulkIds[BinaryDocument](bulk.map(_.id), akkaBucket, classOf[BinaryDocument])
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "delete single element" in assertAllStagesStopped {
      val upsertFuture: Future[Done] =
        Source
          .single(head)
          .map(createCBRawJson)
          .via(
            CouchbaseFlow.upsertSingle(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                       akkaBucket)
          )
          .runWith(Sink.ignore)
      //wait til operation completed
      Await.result(upsertFuture, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] =
        Source
          .single(head.id)
          .via(
            CouchbaseFlow.deleteOne(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                    akkaBucket)
          )
          .runWith(Sink.ignore)
      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[RawJsonDocument] =
        CouchbaseSource
          .fromSingleId[RawJsonDocument](head.id, akkaBucket, classOf[RawJsonDocument])
          .runWith(Sink.head[RawJsonDocument])
      Await.result(getFuture, Duration(5, TimeUnit.SECONDS)) must throwA[NoSuchElementException]
    }

    "delete bulk of elements" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] = Source
        .fromIterator(() => bulk.map(_.id).iterator)
        .grouped(2)
        .via(CouchbaseFlow.deleteBulk(CouchbaseWriteSettings().withParallelism(2), akkaBucket))
        .runWith(Sink.ignore)

      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[Seq[RawJsonDocument]] =
        CouchbaseSource
          .fromBulkIds[RawJsonDocument](bulk.map(_.id), akkaBucket, classOf[RawJsonDocument])
          .runWith(Sink.seq[RawJsonDocument])
      val getResult: Seq[RawJsonDocument] = Await.result(getFuture, Duration(5, TimeUnit.SECONDS))
      getResult.length shouldEqual 0
    }

    "delete bulk of elements and not all exists" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBRawJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val deleteFuture: Future[Done] = Source
        .fromIterator(() => ("NoneExisting" +: bulk.map(_.id)).iterator)
        .grouped(2)
        .via(CouchbaseFlow.deleteBulk(CouchbaseWriteSettings().withParallelism(2), akkaBucket))
        .runWith(Sink.ignore)
      Await.result(deleteFuture, Duration(5, TimeUnit.SECONDS))

      val getFuture: Future[Seq[RawJsonDocument]] =
        CouchbaseSource
          .fromBulkIds[RawJsonDocument](bulk.map(_.id), akkaBucket, classOf[RawJsonDocument])
          .runWith(Sink.seq[RawJsonDocument])
      val getResult: Seq[RawJsonDocument] = Await.result(getFuture, Duration(5, TimeUnit.SECONDS))

      getResult.length shouldEqual 0
    }

    "get single document as part of the flow" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val id = "First"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(CouchbaseFlow.fromSingleId(akkaBucket, classOf[JsonDocument]))
        .runWith(Sink.head[JsonDocument])
      Await.result(result, Duration(5, TimeUnit.SECONDS)).id() shouldEqual id
    }

    "get single document as part of the flow for not existing id" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val id = "not exists"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(CouchbaseFlow.fromSingleId(akkaBucket, classOf[JsonDocument]))
        .runWith(Sink.head[JsonDocument])
      Await.result(result, Duration(5, TimeUnit.SECONDS)) must throwA[NoSuchElementException]
    }

    "get bulk of documents as part of the flow" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val result: Future[Seq[JsonDocument]] = Source
        .single(bulk.map(_.id))
        .via(CouchbaseFlow.fromBulkIds(akkaBucket, classOf[JsonDocument]))
        .runWith(Sink.head)
      Await.result(result, Duration(5, TimeUnit.SECONDS)).map(_.id) must contain(
        exactly("First", "Second", "Third", "Fourth")
      )
    }

    "produce exception in single operation with ReplicateTo higher then # of nodes" in assertAllStagesStopped {

      val resultAsFuture: Future[SingleOperationResult[JsonDocument]] = Source
        .single(bulk.head)
        .map(createCBJson)
        .via(
          CouchbaseFlow.upsertSingle(CouchbaseWriteSettings()
                                       .withParallelism(2)
                                       .withPersistTo(PersistTo.THREE)
                                       .withTimeout(1.seconds),
                                     akkaBucket)
        )
        .runWith(Sink.head)

      val response: SingleOperationResult[JsonDocument] = Await.result(resultAsFuture, Duration(5, TimeUnit.SECONDS))

      response.result.isFailure shouldEqual true

    }
    "produce exceptions in operation with ReplicateTo higher then #of nodes" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Seq[BulkOperationResult[JsonDocument]]] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings()
                                     .withParallelism(2)
                                     .withPersistTo(PersistTo.THREE)
                                     .withTimeout(1.seconds),
                                   akkaBucket)
        )
        .runWith(Sink.seq[BulkOperationResult[JsonDocument]])

      val entities: Seq[BulkOperationResult[JsonDocument]] =
        Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))
      val failures: Seq[FailedOperation] = entities.flatMap { f =>
        f.failures
      }
      failures.length shouldEqual 4
    }

    "get bulk of documents as part of the flownot all ids exists" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => bulk.iterator)
        .map(createCBJson)
        .grouped(2)
        .via(
          CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withParallelism(2).withPersistTo(PersistTo.NONE),
                                   akkaBucket)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))

      val result: Future[Seq[JsonDocument]] = Source
        .single(bulk.map(_.id) :+ "Not Existing Id")
        .via(CouchbaseFlow.fromBulkIds(akkaBucket, classOf[JsonDocument]))
        .runWith(Sink.head)
      Await.result(result, Duration(5, TimeUnit.SECONDS)).map(_.id) must contain(
        exactly("First", "Second", "Third", "Fourth")
      )
    }
  }

  override protected def after: Any = cleanAllInBucket(bulk.map(_.id), akkaBucket)
}
