/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Sink, Source}

//#write-settings
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import com.couchbase.client.java.{PersistTo, ReplicateTo}
//#write-settings

import akka.stream.testkit.scaladsl.StreamTestKit._
import com.couchbase.client.java.document.{BinaryDocument, RawJsonDocument, StringDocument}
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterEach, BeforeAfterAll}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//#init-sourceBulk
import com.couchbase.client.java.document.JsonDocument

//#init-sourceBulk

class CouchbaseFlowSpec extends Specification with BeforeAfterAll with AfterEach with CouchbaseSupport with Matchers {

  sequential

  "Couchbase Flow" should {

    "create default writeSettings object" in assertAllStagesStopped {

      val writeSettings = CouchbaseWriteSettings()
      val expectedwriteSettings = CouchbaseWriteSettings(1, ReplicateTo.NONE, PersistTo.NONE, 2.seconds)
      writeSettings shouldEqual expectedwriteSettings
    }

    "create custom writeSettings object" in {

      //#write-settings
      val writeSettings = CouchbaseWriteSettings()
        .withParallelism(3)
        .withPersistTo(PersistTo.FOUR)
        .withReplicateTo(ReplicateTo.THREE)
        .withTimeout(5.seconds)
      //#write-settings

      val expectedwriteSettings = CouchbaseWriteSettings(3, ReplicateTo.THREE, PersistTo.FOUR, 5.seconds)
      writeSettings shouldEqual expectedwriteSettings
    }

    "insert RawJsonDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(toRawJsonDocument)
          .via(
            CouchbaseFlow.upsertDoc(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      Await.result(result, 5.seconds)

      val msgFuture: Future[RawJsonDocument] =
        CouchbaseSource
          .fromId(sessionSettings, sampleData.id, bucketName, classOf[RawJsonDocument])
          .runWith(Sink.head)
      Await.result(msgFuture, 5.seconds).id() shouldEqual sampleData.id

    }

    "insert JsonDocument" in assertAllStagesStopped {

      // #by-single-id-flow
      import akka.Done

      val obj = TestObject("First", "First")

      val result: Future[Done] =
        Source
          .single(obj)
          .map(toJsonDocument)
          .via(CouchbaseFlow.upsert(sessionSettings, writeSettings, bucketName))
          .runWith(Sink.ignore)

      // #by-single-id-flow
      Await.result(result, 5.seconds)

      //#init-sourceSingle
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource
      import com.couchbase.client.java.document.JsonDocument

      val id: String = "First"

      val msgFuture: Future[JsonDocument] =
        CouchbaseSource
          .fromId(sessionSettings, id, bucketName)
          .runWith(Sink.head)
      //#init-sourceSingle
      Await.result(msgFuture, 5.seconds).id() shouldEqual sampleData.id
    }

    "insert StringDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(toStringDocument)
          .via(CouchbaseFlow.upsertDoc(sessionSettings, writeSettings, bucketName))
          .runWith(Sink.ignore)
      Await.result(result, 5.seconds)

      val msgFuture: Future[StringDocument] =
        CouchbaseSource
          .fromId(sessionSettings, sampleData.id, bucketName, classOf[StringDocument])
          .runWith(Sink.head)
      Await.result(msgFuture, 5.seconds).id() shouldEqual sampleData.id
    }

    "insert BinaryDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(toBinaryDocument)
          .via(CouchbaseFlow.upsertDoc(sessionSettings, writeSettings, bucketName))
          .runWith(Sink.ignore)
      Await.result(result, 5.seconds)

      val msgFuture: Future[BinaryDocument] =
        CouchbaseSource
          .fromId(sessionSettings, sampleData.id, bucketName, classOf[BinaryDocument])
          .runWith(Sink.head)
      Await.result(msgFuture, 5.seconds).id() shouldEqual sampleData.id
    }

    "insert multiple RawJsonDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toRawJsonDocument)
        .via(
          CouchbaseFlow.upsertDoc(
            sessionSettings,
            writeSettings.withParallelism(2),
            bucketName
          )
        )
        .runWith(Sink.ignore)

      Await.result(bulkUpsertResult, 15.seconds)

      val resultsAsFuture: Future[immutable.Seq[RawJsonDocument]] =
        Source(sampleSequence.map(_.id))
          .via(CouchbaseFlow.fromId(sessionSettings, bucketName, classOf[RawJsonDocument]))
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, 5.seconds)

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multiple JsonDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)

      Await.result(bulkUpsertResult, 5.seconds)

      //#init-sourceBulk
      val ids: Seq[String] = Seq("First", "Second", "Third", "Fourth")

      val futureResult: Future[immutable.Seq[JsonDocument]] =
        Source(ids)
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName
            )
          )
          .runWith(Sink.seq)
      //#init-sourceBulk

      val result = Await.result(futureResult, 5.seconds)

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multiple StringDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toStringDocument)
        .via(
          CouchbaseFlow.upsertDoc(
            sessionSettings,
            writeSettings.withParallelism(2),
            bucketName
          )
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val resultsAsFuture: Future[immutable.Seq[StringDocument]] =
        Source(sampleSequence.map(_.id))
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName,
              classOf[StringDocument]
            )
          )
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, 5.seconds)

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "insert multiple BinaryDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toBinaryDocument)
        .via(
          CouchbaseFlow.upsertDoc(
            sessionSettings,
            writeSettings.withParallelism(2),
            bucketName
          )
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val resultsAsFuture: Future[immutable.Seq[BinaryDocument]] =
        Source(sampleSequence.map(_.id))
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName,
              classOf[BinaryDocument]
            )
          )
          .runWith(Sink.seq)
      val result = Await.result(resultsAsFuture, 5.seconds)

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "delete single element" in assertAllStagesStopped {
      val upsertFuture: Future[Done] =
        Source
          .single(sampleData)
          .map(toRawJsonDocument)
          .via(
            CouchbaseFlow.upsertDoc(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      //wait til operation completed
      Await.result(upsertFuture, 5.seconds)

      val deleteFuture: Future[Done] =
        Source
          .single(sampleData.id)
          .via(
            CouchbaseFlow.delete(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      Await.result(deleteFuture, 5.seconds)

      val getFuture: Future[RawJsonDocument] =
        CouchbaseSource
          .fromId(
            sessionSettings,
            sampleData.id,
            bucketName,
            classOf[RawJsonDocument]
          )
          .runWith(Sink.head)
      Await.result(getFuture, 5.seconds) must throwA[NoSuchElementException]
    }

    "delete elements and some do not exist" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toRawJsonDocument)
        .via(
          CouchbaseFlow.upsertDoc(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val deleteFuture: Future[Done] = Source("NoneExisting" +: sampleSequence.map(_.id))
        .via(
          CouchbaseFlow.delete(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(deleteFuture, 5.seconds)

      val getFuture: Future[Seq[RawJsonDocument]] =
        Source(sampleSequence.map(_.id))
          .via(
            CouchbaseFlow.fromId(sessionSettings, bucketName, classOf[RawJsonDocument])
          )
          .runWith(Sink.seq)
      Await.result(getFuture, 5.seconds).length shouldEqual 0
    }

    "get document in flow" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => sampleSequence.iterator)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val id = "First"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(
          CouchbaseFlow.fromId(sessionSettings, bucketName)
        )
        .runWith(Sink.head)
      Await.result(result, 5.seconds).id() shouldEqual id
    }

    "get document in flow that does not exist" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val id = "not exists"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(CouchbaseFlow.fromId(sessionSettings, bucketName))
        .runWith(Sink.head)
      Await.result(result, 5.seconds) must throwA[NoSuchElementException]
    }

    "get bulk of documents as part of the flow" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val result: Future[Seq[JsonDocument]] = Source(sampleSequence.map(_.id))
        .via(CouchbaseFlow.fromId(sessionSettings, bucketName))
        .runWith(Sink.seq)
      Await.result(result, 5.seconds).map(_.id) must contain(
        exactly("First", "Second", "Third", "Fourth")
      )
    }

    "fails stream when ReplicateTo higher then #of nodes" in assertAllStagesStopped {
      val bulkUpsertResult: Future[immutable.Seq[JsonDocument]] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings,
                               writeSettings
                                 .withParallelism(2)
                                 .withPersistTo(PersistTo.THREE)
                                 .withTimeout(1.seconds),
                               bucketName)
        )
        .runWith(Sink.seq)

      Await.result(bulkUpsertResult, 5.seconds) must throwA[com.couchbase.client.java.error.DurabilityException]
    }

    "get bulk of documents as part of the flow where not all ids exist" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source
        .fromIterator(() => sampleSequence.iterator)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      Await.result(bulkUpsertResult, 5.seconds)

      val result: Future[Seq[JsonDocument]] = Source
        .apply(sampleSequence.map(_.id) :+ "Not Existing Id")
        .via(CouchbaseFlow.fromId(sessionSettings, bucketName))
        .runWith(Sink.seq)
      Await.result(result, 5.seconds).map(_.id) must contain(
        exactly("First", "Second", "Third", "Fourth")
      )
    }
  }

  override protected def after: Any = cleanAllInBucket(sampleSequence.map(_.id), bucketName)
}
