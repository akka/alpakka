/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
import akka.stream.alpakka.couchbase.testing.CouchbaseSupport
import akka.stream.alpakka.couchbase.{CouchbaseDeleteFailure, CouchbaseDeleteResult, CouchbaseDocument}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit._
import com.couchbase.client.core.error.{DocumentNotFoundException, DurabilityImpossibleException}
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.java.codec.RawBinaryTranscoder
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.kv.{RemoveOptions, UpsertOptions}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

//#init-sourceBulk
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

  "Couchbase upsert" should {

    "insert RawJsonDocument" in assertAllStagesStopped {
      // #upsert
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(data => new CouchbaseDocument(data.getId, data.getDocument.getBytes()))
          .via(
            CouchbaseFlow.upsert(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      result.futureValue

      // #upsert

      val msgFuture =
        session.collection(scopeName, collectionName).getBytes(sampleData.getId, FiniteDuration.apply(2, SECONDS))

      msgFuture.futureValue.getId shouldEqual sampleData.getId
      msgFuture.futureValue.getDocument shouldEqual sampleData.getDocument.getBytes()

    }

    "insert JsonDocument" in assertAllStagesStopped {
      // #upsert
      val jsonDocumentUpsert: Future[Done] =
        Source
          .single(sampleData)
          .map(doc => new CouchbaseDocument(doc.getId, JsonObject.create().put("value", doc.getDocument)))
          .via(
            CouchbaseFlow.upsert(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      jsonDocumentUpsert.futureValue

      // #upsert

      val msgFuture = session.collection(scopeName, collectionName).get(sampleData.getId, classOf[JsonObject])
      msgFuture.futureValue.getDocument.get("value") shouldEqual sampleData.getDocument
    }

    "insert StringDocument" in assertAllStagesStopped {
      // #upsert

      val stringDocumentUpsert: Future[Done] =
        Source
          .single(sampleData)
          .via(
            CouchbaseFlow.upsert(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      stringDocumentUpsert.futureValue

      // #upsert

      val msgFuture = session.collection(scopeName, collectionName).get(sampleData.getId, classOf[String])
      msgFuture.futureValue.getId shouldEqual sampleData.getId
      msgFuture.futureValue.getDocument shouldEqual sampleData.getDocument
    }

    "insert BinaryDocument" in assertAllStagesStopped {
      // #upsert
      val result: Future[Done] =
        Source
          .single(sampleData)
          .map(doc => new CouchbaseDocument(doc.getId, doc.getDocument.getBytes))
          .via(CouchbaseFlow.upsert(sessionSettings, bucketName, scopeName, collectionName))
          .runWith(Sink.ignore)
      result.futureValue

      // #upsert

      val msgFuture = session.collection(scopeName, collectionName).getBytes(sampleData.getId)
      msgFuture.futureValue.getId shouldEqual sampleData.getId
      msgFuture.futureValue.getDocument shouldEqual sampleData.getDocument.getBytes
    }

    "insert multiple RawJsonDocuments" in assertAllStagesStopped {
      // #upsert
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(doc => new CouchbaseDocument(doc.getId, JsonObject.create().put("value", doc.getDocument)))
        .via(
          CouchbaseFlow.upsert(
            sessionSettings,
            bucketName,
            scopeName,
            collectionName
          )
        )
        .runWith(Sink.ignore)

      bulkUpsertResult.futureValue

      // #upsert

      val resultsAsFuture =
        Source(sampleSequence.map(_.getId))
          .via(CouchbaseFlow.fromId(sessionSettings, bucketName, scopeName, collectionName))
          .runWith(Sink.seq)

      resultsAsFuture.futureValue.map(_.getId) should contain.inOrderOnly("First", "Second", "Third", "Fourth")
    }

    "insert multiple JsonDocuments" in assertAllStagesStopped {
      // #upsert
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(doc => new CouchbaseDocument(doc.getId, JsonObject.create().put("value", doc.getDocument)))
        .via(
          CouchbaseFlow.upsert(sessionSettings, bucketName, scopeName, collectionName)
        )
        .runWith(Sink.ignore)

      bulkUpsertResult.futureValue

      // #upsert

      // #fromId
      val ids = immutable.Seq("First", "Second", "Third", "Fourth")

      val futureResult =
        Source(ids)
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName,
              classOf[JsonObject]
            )
          )
          .runWith(Sink.seq)

      // #fromId

      futureResult.futureValue.map(_.getId) should contain.inOrderOnly("First", "Second", "Third", "Fourth")
    }

    "insert multiple StringDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .via(
          CouchbaseFlow.upsert(
            sessionSettings,
            bucketName,
            scopeName,
            collectionName
          )
        )
        .runWith(Sink.ignore)
      bulkUpsertResult.futureValue

      val resultsAsFuture =
        Source(sampleSequence.map(_.getId))
          .via(
            CouchbaseFlow.fromId(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName,
              classOf[String]
            )
          )
          .runWith(Sink.seq)

      resultsAsFuture.futureValue.map(_.getId) should contain.inOrder("First", "Second", "Third", "Fourth")
    }

    "insert multiple BinaryDocuments" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .map(doc => new CouchbaseDocument(doc.getId, doc.getDocument.getBytes))
        .via(
          CouchbaseFlow.upsert(
            sessionSettings,
            bucketName,
            scopeName,
            collectionName
          )
        )
        .runWith(Sink.ignore)
      bulkUpsertResult.futureValue

      // #fromId
      val resultsAsFuture =
        Source(sampleSequence.map(_.getId))
          .via(
            CouchbaseFlow.bytesFromId(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.seq)
      // #fromId
      resultsAsFuture.futureValue.map(_.getId) shouldBe Seq("First", "Second", "Third", "Fourth")
    }

  }

  "Couchbase delete" should {

    "delete single element" in assertAllStagesStopped {
      val upsertFuture: Future[Done] =
        Source
          .single(sampleData)
          .via(
            CouchbaseFlow.upsert(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      //wait til operation completed
      upsertFuture.futureValue

      // #delete
      val deleteFuture: Future[Done] =
        Source
          .single(sampleData.getId)
          .via(
            CouchbaseFlow.delete(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      // #delete
      deleteFuture.futureValue

      Thread.sleep(1000)

      val msgFuture = session.collection(scopeName, collectionName).get(sampleData.getId, classOf[String])
      msgFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]

      val getFuture =
        Source
          .single(sampleData.getId)
          .via(
            CouchbaseFlow
              .fromId(
                sessionSettings,
                bucketName,
                scopeName,
                collectionName
              )
          )
          .runWith(Sink.head)
      getFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]
    }

    "delete elements and some do not exist" in assertAllStagesStopped {
      val bulkUpsertResult: Future[Done] = Source(sampleSequence)
        .via(
          CouchbaseFlow.upsert(sessionSettings, bucketName, scopeName, collectionName)
        )
        .runWith(Sink.ignore)
      bulkUpsertResult.futureValue

      val deleteFuture: Future[Done] = Source(sampleSequence.map(_.getId) :+ "NoneExisting")
        .via(
          CouchbaseFlow.delete(sessionSettings, bucketName, scopeName, collectionName)
        )
        .runWith(Sink.ignore)
      deleteFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]

      val getFuture =
        Source(sampleSequence.map(_.getId))
          .via(
            CouchbaseFlow.fromId(sessionSettings, bucketName, scopeName, collectionName)
          )
          .runWith(Sink.seq)
      getFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]
    }
  }

  "Couchbase get" should {

    "get document in flow" in assertAllStagesStopped {
      upsertSampleData(bucketName, scopeName, collectionName)

      val id = "First"

      val result = Source
        .single(id)
        .via(
          CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName)
        )
        .runWith(Sink.head)
      result.futureValue.getId shouldEqual id
    }

    "get document in flow that does not exist" in assertAllStagesStopped {
      val id = "not exists"

      val result = Source
        .single(id)
        .via(CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName))
        .runWith(Sink.head)
      result.failed.futureValue shouldBe a[NoSuchElementException]
    }

    "get bulk of documents as part of the flow" in assertAllStagesStopped {
      upsertSampleData(bucketName, scopeName, collectionName)

      val result = Source(sampleSequence.map(_.getId))
        .via(CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName))
        .runWith(Sink.seq)
      result.futureValue.map(_.getId) shouldBe Seq("First", "Second", "Third", "Fourth")
    }

    "get bulk of documents as part of the flow where not all ids exist" in assertAllStagesStopped {
      upsertSampleData(bucketName, scopeName, collectionName)

      val result = Source
        .apply(sampleSequence.map(_.getId) :+ "Not Existing Id")
        .via(CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName))
        .runWith(Sink.seq)

      result.futureValue.map(_.getId) shouldBe Seq("First", "Second", "Third", "Fourth")
    }
  }

  "Couchbase replace" should {

    "replace single element" in assertAllStagesStopped {
      upsertSampleData(bucketName, scopeName, collectionName)

      val obj = new CouchbaseDocument("Second", "SecondReplace")

      // #replace
      val replaceFuture: Future[Done] =
        Source
          .single(obj)
          .map(doc => new CouchbaseDocument(doc.getId, JsonObject.create().put("value", doc.getDocument)))
          .via(
            CouchbaseFlow.replace(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.ignore)
      // #replace
      replaceFuture.futureValue

      Thread.sleep(1000)

      val msgFuture = session.collection(scopeName, collectionName).getJsonObject(obj.getId)
      msgFuture.futureValue.getDocument.get("value") shouldEqual obj.getDocument
    }

    "replace multiple RawJsonDocuments" in assertAllStagesStopped {

      val replaceSequence = sampleData +: Seq[CouchbaseDocument[String]](
          new CouchbaseDocument("Second", "SecondReplace"),
          new CouchbaseDocument("Third", "ThirdReplace"),
          new CouchbaseDocument("Fourth", "FourthReplace")
        )

      upsertSampleData(bucketName, scopeName, collectionName)

      val bulkReplaceResult: Future[Done] = Source(replaceSequence)
        .map(doc => new CouchbaseDocument(doc.getId, doc.getDocument.getBytes))
        .via(
          CouchbaseFlow.replace(
            sessionSettings,
            bucketName,
            scopeName,
            collectionName
          )
        )
        .runWith(Sink.ignore)

      bulkReplaceResult.futureValue

      val resultsAsFuture =
        Source(sampleSequence.map(_.getId))
          .via(CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName))
          .runWith(Sink.seq)

      resultsAsFuture.futureValue.map(doc => new String(doc.getDocument)) should contain.inOrderOnly("First",
                                                                                                     "SecondReplace",
                                                                                                     "ThirdReplace",
                                                                                                     "FourthReplace")
    }

    "replace RawJsonDocument" in assertAllStagesStopped {

      upsertSampleData(bucketName, scopeName, collectionName)

      val obj = new CouchbaseDocument("Second", "SecondReplace")

      // #replaceDoc
      val replaceDocFuture: Future[Done] = Source
        .single(obj)
        .via(
          CouchbaseFlow.replace(
            sessionSettings,
            bucketName,
            scopeName,
            collectionName
          )
        )
        .runWith(Sink.ignore)
      // #replaceDocreplace

      replaceDocFuture.futureValue

      Thread.sleep(1000)

      val msgFuture = session.collection(scopeName, collectionName).get(obj.getId, classOf[String])
      msgFuture.futureValue.getDocument shouldEqual obj.getDocument
    }

    "Couchbase upsert with result" should {
      "write documents" in assertAllStagesStopped {
        // #upsertWithResult
        import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

        val result: Future[immutable.Seq[CouchbaseWriteResult]] =
          Source(sampleSequence)
            .map(doc => new CouchbaseDocument(doc.getId, doc.getDocument.getBytes))
            .via(
              CouchbaseFlow.upsertWithResult(
                sessionSettings,
                bucketName,
                scopeName,
                collectionName
              )
            )
            .runWith(Sink.seq)

        val failedDocs: immutable.Seq[CouchbaseWriteFailure] = result.futureValue.collect {
          case res: CouchbaseWriteFailure => res
        }

        // #upsertWithResult

        result.futureValue should have size sampleSequence.size
        failedDocs shouldBe empty
        forAll(result.futureValue)(_ shouldBe Symbol("success"))
      }

      "expose failures in-stream" in assertAllStagesStopped {
        // #upsertWithResult
        import akka.stream.alpakka.couchbase.{CouchbaseWriteFailure, CouchbaseWriteResult}

        val result: Future[immutable.Seq[CouchbaseWriteResult]] = Source(sampleSequence)
          .map(doc => new CouchbaseDocument(doc.getId, doc.getDocument.getBytes))
          .via(
            CouchbaseFlow.upsertWithResult(
              sessionSettings,
              UpsertOptions
                .upsertOptions()
                .transcoder(RawBinaryTranscoder.INSTANCE)
                .durability(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
                .timeout(java.time.Duration.ofSeconds(1)),
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.seq)

        // #upsertWithResult

        result.futureValue should have size sampleSequence.size
        val failedDocs: immutable.Seq[CouchbaseWriteFailure] = result.futureValue.collect {
          case res: CouchbaseWriteFailure => res
        }
        failedDocs.head.failure shouldBe a[DurabilityImpossibleException]
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
                RemoveOptions
                  .removeOptions()
                  .durability(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
                  .timeout(java.time.Duration.ofSeconds(1)),
                bucketName,
                scopeName,
                collectionName
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
                RemoveOptions
                  .removeOptions()
                  .durability(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
                  .timeout(java.time.Duration.ofSeconds(1)),
                bucketName,
                scopeName,
                collectionName
              )
            )
            .runWith(Sink.head)
        // #deleteWithResult
        deleteFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]

        deleteResult.futureValue shouldBe a[CouchbaseDeleteFailure]
        deleteResult.futureValue.id shouldBe "non-existent"
        deleteResult.mapTo[CouchbaseDeleteFailure].futureValue.failure shouldBe a[DocumentNotFoundException]
      }
    }

    "replace documents" in assertAllStagesStopped {

      upsertSampleData(bucketName, scopeName, collectionName)

      // #replaceWithResult
      import akka.stream.alpakka.couchbase.CouchbaseWriteFailure

      val result =
        Source(sampleSequence)
          .via(
            CouchbaseFlow.replaceWithResult(
              sessionSettings,
              bucketName,
              scopeName,
              collectionName
            )
          )
          .runWith(Sink.seq)

      val failedDocs = result.futureValue.collect {
        case res: CouchbaseWriteFailure => res
      }
      // #replaceWithResult

      result.futureValue should have size sampleSequence.size
      failedDocs shouldBe empty
      forAll(result.futureValue)(_ shouldBe Symbol("success"))
    }

    "expose failures in-stream" in assertAllStagesStopped {

      cleanAllInCollection(bucketName, scopeName, collectionName)

      import akka.stream.alpakka.couchbase.CouchbaseWriteFailure

      val result = Source(sampleSequence)
        .via(
          CouchbaseFlow.replaceWithResult(sessionSettings, bucketName, scopeName, collectionName)
        )
        .runWith(Sink.seq)

      result.futureValue should have size sampleSequence.size
      val failedDocs: immutable.Seq[CouchbaseWriteFailure] = result.futureValue.collect {
        case res: CouchbaseWriteFailure => res
      }
      failedDocs.head.failure shouldBe a[DocumentNotFoundException]
    }
  }
}
