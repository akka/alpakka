/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase.{CouchbaseDeleteFailure, CouchbaseDeleteResult}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
import akka.stream.alpakka.couchbase.testing.{CouchbaseSupport, TestObject}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.error.DocumentDoesNotExistException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

//#write-settings
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import com.couchbase.client.java.{PersistTo, ReplicateTo}
//#write-settings

import akka.stream.testkit.scaladsl.StreamTestKit._
import com.couchbase.client.java.document.{BinaryDocument, RawJsonDocument, StringDocument}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.Future

//#init-sourceBulk
import com.couchbase.client.java.document.JsonDocument
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

//#init-sourceBulk

class CouchbaseFlowSpec
    extends AnyWordSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with CouchbaseSupport
    with Matchers
    with ScalaFutures
    with Inspectors
    with LogCapturing {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  override def beforeAll(): Unit = super.beforeAll()
  override def afterAll(): Unit = super.afterAll()

  "Couchbase settings" should {

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
  }

  "Couchbase upsert" should {

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
      result.futureValue

      val msgFuture: Future[Option[RawJsonDocument]] = session.get(sampleData.id, classOf[RawJsonDocument])
      msgFuture.futureValue.get.id() shouldEqual sampleData.id

    }

    "insert JsonDocument" in assertAllStagesStopped {

      // #upsert
      val obj = TestObject(id = "First", "First")

      val writeSettings = CouchbaseWriteSettings()

      val jsonDocumentUpsert: Future[Done] =
        Source
          .single(obj)
          .map(toJsonDocument)
          .via(
            CouchbaseFlow.upsert(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      // #upsert
      jsonDocumentUpsert.futureValue

      val msgFuture: Future[Option[JsonDocument]] = session.get(obj.id)
      msgFuture.futureValue.get.content().get("value") shouldEqual obj.value
    }

    "insert StringDocument" in assertAllStagesStopped {
      // #upsert

      val stringDocumentUpsert: Future[Done] =
        Source
          .single(sampleData)
          .map(toStringDocument)
          .via(
            CouchbaseFlow.upsertDoc(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      // #upsert
      stringDocumentUpsert.futureValue

      val msgFuture: Future[Option[StringDocument]] = session.get(sampleData.id, classOf[StringDocument])
      msgFuture.futureValue.get.id() shouldEqual sampleData.id
    }

    "insert BinaryDocument" in assertAllStagesStopped {
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(toBinaryDocument)
          .via(CouchbaseFlow.upsertDoc(sessionSettings, writeSettings, bucketName))
          .runWith(Sink.ignore)
      result.futureValue

      val msgFuture: Future[Option[BinaryDocument]] = session.get(sampleData.id, classOf[BinaryDocument])
      msgFuture.futureValue.get.id() shouldEqual sampleData.id
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

      bulkUpsertResult.futureValue

      val resultsAsFuture: Future[immutable.Seq[RawJsonDocument]] =
        Source(sampleSequence.map(_.id))
          .via(CouchbaseFlow.fromId(sessionSettings, bucketName, classOf[RawJsonDocument]))
          .runWith(Sink.seq)

      resultsAsFuture.futureValue.map(_.id()) should contain inOrderOnly ("First", "Second", "Third", "Fourth")
    }

    "insert multiple JsonDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsert(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)

      bulkUpsertResult.futureValue

      // #fromId
      val ids = immutable.Seq("First", "Second", "Third", "Fourth")

      val futureResult: Future[immutable.Seq[JsonDocument]] =
        Source(ids)
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName
            )
          )
          .runWith(Sink.seq)
      // #fromId

      futureResult.futureValue.map(_.id()) should contain inOrderOnly ("First", "Second", "Third", "Fourth")
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
      bulkUpsertResult.futureValue

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

      resultsAsFuture.futureValue.map(_.id()) should contain inOrder ("First", "Second", "Third", "Fourth")
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
      bulkUpsertResult.futureValue

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
      resultsAsFuture.futureValue.map(_.id()) shouldBe Seq("First", "Second", "Third", "Fourth")
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

      bulkUpsertResult.failed.futureValue shouldBe a[com.couchbase.client.java.error.DurabilityException]
    }
  }

  "Couchbase delete" should {

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
      upsertFuture.futureValue

      // #delete
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
      // #delete
      deleteFuture.futureValue

      Thread.sleep(1000)

      val msgFuture: Future[Option[RawJsonDocument]] = session.get(sampleData.id, classOf[RawJsonDocument])
      msgFuture.futureValue shouldBe 'empty

      val getFuture: Future[RawJsonDocument] =
        Source
          .single(sampleData.id)
          .via(
            CouchbaseFlow
              .fromId(
                sessionSettings,
                bucketName,
                classOf[RawJsonDocument]
              )
          )
          .runWith(Sink.head)
      getFuture.failed.futureValue shouldBe a[NoSuchElementException]
    }

    "delete elements and some do not exist" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(toRawJsonDocument)
        .via(
          CouchbaseFlow.upsertDoc(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      bulkUpsertResult.futureValue

      val deleteFuture: Future[Done] = Source(sampleSequence.map(_.id) :+ "NoneExisting")
        .via(
          CouchbaseFlow.delete(sessionSettings, writeSettings.withParallelism(2), bucketName)
        )
        .runWith(Sink.ignore)
      deleteFuture.failed.futureValue shouldBe a[DocumentDoesNotExistException]

      val getFuture: Future[Seq[RawJsonDocument]] =
        Source(sampleSequence.map(_.id))
          .via(
            CouchbaseFlow.fromId(sessionSettings, bucketName, classOf[RawJsonDocument])
          )
          .runWith(Sink.seq)
      getFuture.futureValue shouldBe 'empty
    }
  }

  "Couchbase get" should {

    "get document in flow" in assertAllStagesStopped {
      upsertSampleData(queryBucketName)

      val id = "First"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(
          CouchbaseFlow.fromId(sessionSettings, queryBucketName)
        )
        .runWith(Sink.head)
      result.futureValue.id() shouldEqual id
    }

    "get document in flow that does not exist" in assertAllStagesStopped {
      val id = "not exists"

      val result: Future[JsonDocument] = Source
        .single(id)
        .via(CouchbaseFlow.fromId(sessionSettings, bucketName))
        .runWith(Sink.head)
      result.failed.futureValue shouldBe a[NoSuchElementException]
    }

    "get bulk of documents as part of the flow" in assertAllStagesStopped {
      upsertSampleData(queryBucketName)

      val result: Future[Seq[JsonDocument]] = Source(sampleSequence.map(_.id))
        .via(CouchbaseFlow.fromId(sessionSettings, queryBucketName))
        .runWith(Sink.seq)
      result.futureValue.map(_.id) shouldBe Seq("First", "Second", "Third", "Fourth")
    }

    "get bulk of documents as part of the flow where not all ids exist" in assertAllStagesStopped {
      upsertSampleData(queryBucketName)

      val result: Future[Seq[JsonDocument]] = Source
        .apply(sampleSequence.map(_.id) :+ "Not Existing Id")
        .via(CouchbaseFlow.fromId(sessionSettings, queryBucketName))
        .runWith(Sink.seq)
      result.futureValue.map(_.id) shouldBe Seq("First", "Second", "Third", "Fourth")
    }
  }

  "Couchbase replace" should {

    "replace single element" in assertAllStagesStopped {
      upsertSampleData(bucketName)

      val obj = TestObject("Second", "SecondReplace")

      // #replace
      val replaceFuture: Future[Done] =
        Source
          .single(obj)
          .map(toJsonDocument)
          .via(
            CouchbaseFlow.replace(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.ignore)
      // #replace
      replaceFuture.futureValue

      Thread.sleep(1000)

      val msgFuture: Future[Option[JsonDocument]] = session.get(obj.id)
      msgFuture.futureValue.get.content().get("value") shouldEqual obj.value
    }

    "replace multiple RawJsonDocuments" in assertAllStagesStopped {

      val replaceSequence: Seq[TestObject] = sampleData +: Seq[TestObject](TestObject("Second", "SecondReplace"),
                                                                           TestObject("Third", "ThirdReplace"))

      upsertSampleData(bucketName)

      val bulkReplaceResult: Future[Done] = Source(replaceSequence)
        .map(toRawJsonDocument)
        .via(
          CouchbaseFlow.replaceDoc(
            sessionSettings,
            writeSettings.withParallelism(2),
            bucketName
          )
        )
        .runWith(Sink.ignore)

      bulkReplaceResult.futureValue

      val resultsAsFuture: Future[immutable.Seq[JsonDocument]] =
        Source(sampleSequence.map(_.id))
          .via(CouchbaseFlow.fromId(sessionSettings, bucketName))
          .runWith(Sink.seq)

      resultsAsFuture.futureValue.map(doc => doc.content().get("value")) should contain inOrderOnly ("First", "SecondReplace", "ThirdReplace", "Fourth")
    }

    "replace RawJsonDocument" in assertAllStagesStopped {

      upsertSampleData(bucketName)

      val obj = TestObject("Second", "SecondReplace")

      // #replaceDoc
      val replaceDocFuture: Future[Done] = Source
        .single(obj)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.replaceDoc(
            sessionSettings,
            writeSettings,
            bucketName
          )
        )
        .runWith(Sink.ignore)
      // #replaceDocreplace

      replaceDocFuture.futureValue

      Thread.sleep(1000)

      val msgFuture: Future[Option[JsonDocument]] = session.get(obj.id)
      msgFuture.futureValue.get.content().get("value") shouldEqual obj.value
    }

    "fails stream when ReplicateTo higher then #of nodes" in assertAllStagesStopped {

      upsertSampleData(bucketName)

      val bulkReplaceResult: Future[immutable.Seq[JsonDocument]] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.replace(sessionSettings,
                                writeSettings
                                  .withParallelism(2)
                                  .withPersistTo(PersistTo.THREE)
                                  .withTimeout(1.seconds),
                                bucketName)
        )
        .runWith(Sink.seq)

      bulkReplaceResult.failed.futureValue shouldBe a[com.couchbase.client.java.error.DurabilityException]
    }
  }

  "Couchbase upsert with result" should {
    "write documents" in assertAllStagesStopped {
      // #upsertDocWithResult
      import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

      val result: Future[immutable.Seq[CouchbaseWriteResult[RawJsonDocument]]] =
        Source(sampleSequence)
          .map(toRawJsonDocument)
          .via(
            CouchbaseFlow.upsertDocWithResult(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.seq)

      val failedDocs: immutable.Seq[CouchbaseWriteFailure[RawJsonDocument]] = result.futureValue.collect {
        case res: CouchbaseWriteFailure[RawJsonDocument] => res
      }
      // #upsertDocWithResult

      result.futureValue should have size sampleSequence.size
      failedDocs shouldBe 'empty
      forAll(result.futureValue)(_ shouldBe 'success)
    }

    "expose failures in-stream" in assertAllStagesStopped {
      import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

      val result: Future[immutable.Seq[CouchbaseWriteResult[JsonDocument]]] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.upsertDocWithResult(sessionSettings,
                                            writeSettings
                                              .withParallelism(2)
                                              .withPersistTo(PersistTo.THREE)
                                              .withTimeout(1.seconds),
                                            bucketName)
        )
        .runWith(Sink.seq)

      result.futureValue should have size sampleSequence.size
      val failedDocs: immutable.Seq[CouchbaseWriteFailure[JsonDocument]] = result.futureValue.collect {
        case res: CouchbaseWriteFailure[JsonDocument] => res
      }
      failedDocs.head.failure shouldBe a[com.couchbase.client.java.error.DurabilityException]
    }

  }

  "delete with result" should {
    "propagate an error in-stream" in assertAllStagesStopped {
      val deleteFuture: Future[String] =
        Source
          .single("non-existent")
          .via(
            CouchbaseFlow.delete(
              sessionSettings,
              writeSettings
                .withParallelism(2)
                .withReplicateTo(ReplicateTo.THREE)
                .withTimeout(1.seconds),
              bucketName
            )
          )
          .runWith(Sink.head)

      // #deleteWithResult
      val deleteResult: Future[CouchbaseDeleteResult] =
        Source
          .single("non-existent")
          .via(
            CouchbaseFlow.deleteWithResult(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.head)
      // #deleteWithResult
      deleteFuture.failed.futureValue shouldBe a[DocumentDoesNotExistException]

      deleteResult.futureValue shouldBe a[CouchbaseDeleteFailure]
      deleteResult.futureValue.id shouldBe "non-existent"
      deleteResult.mapTo[CouchbaseDeleteFailure].futureValue.failure shouldBe a[DocumentDoesNotExistException]
    }
  }

  "replace with result" should {
    "replace documents" in assertAllStagesStopped {

      upsertSampleData(bucketName)

      // #replaceDocWithResult
      import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

      val result: Future[immutable.Seq[CouchbaseWriteResult[RawJsonDocument]]] =
        Source(sampleSequence)
          .map(toRawJsonDocument)
          .via(
            CouchbaseFlow.replaceDocWithResult(
              sessionSettings,
              writeSettings,
              bucketName
            )
          )
          .runWith(Sink.seq)

      val failedDocs: immutable.Seq[CouchbaseWriteFailure[RawJsonDocument]] = result.futureValue.collect {
        case res: CouchbaseWriteFailure[RawJsonDocument] => res
      }
      // #replaceDocWithResult

      result.futureValue should have size sampleSequence.size
      failedDocs shouldBe 'empty
      forAll(result.futureValue)(_ shouldBe 'success)
    }

    "expose failures in-stream" in assertAllStagesStopped {

      cleanAllInBucket(bucketName)

      import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

      val result: Future[immutable.Seq[CouchbaseWriteResult[JsonDocument]]] = Source(sampleSequence)
        .map(toJsonDocument)
        .via(
          CouchbaseFlow.replaceDocWithResult(sessionSettings,
                                             writeSettings
                                               .withParallelism(2)
                                               .withPersistTo(PersistTo.THREE)
                                               .withTimeout(1.seconds),
                                             bucketName)
        )
        .runWith(Sink.seq)

      result.futureValue should have size sampleSequence.size
      val failedDocs: immutable.Seq[CouchbaseWriteFailure[JsonDocument]] = result.futureValue.collect {
        case res: CouchbaseWriteFailure[JsonDocument] => res
      }
      failedDocs.head.failure shouldBe a[com.couchbase.client.java.error.DocumentDoesNotExistException]
    }
  }
}
