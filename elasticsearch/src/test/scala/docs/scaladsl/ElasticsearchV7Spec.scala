/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchFlow, ElasticsearchSink, ElasticsearchSource}
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import spray.json.{jsonReader, JsObject, JsString}

import scala.collection.immutable
import scala.concurrent.Future

class ElasticsearchV7Spec extends ElasticsearchSpecBase with ElasticsearchSpecUtils {

  private val connectionSettings: ElasticsearchConnectionSettings = ElasticsearchConnectionSettings(
    "http://localhost:9202"
  )
  private val baseSourceSettings = ElasticsearchSourceSettings(connectionSettings).withApiVersion(ApiVersion.V7)
  private val baseWriteSettings = ElasticsearchWriteSettings(connectionSettings).withApiVersion(ApiVersion.V7)

  override protected def beforeAll() = {
    insertTestData(connectionSettings)
  }

  override def afterAll() = {
    val deleteRequest = HttpRequest(HttpMethods.DELETE)
      .withUri(Uri(connectionSettings.baseUrl).withPath(Path("/_all")))
    http.singleRequest(deleteRequest).futureValue

    TestKit.shutdownActorSystem(system)
  }

  "Un-typed Elasticsearch connector" should {
    "consume and publish Json documents" in assertAllStagesStopped {
      val indexName = "sink2"

      val copy = ElasticsearchSource
        .create(
          constructEsParams("source", "_doc", ApiVersion.V7),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[spray.json.JsObject] =>
          val book: Book = jsonReader[Book].read(message.source)
          WriteMessage.createIndexMessage(message.id, book)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            settings = baseWriteSettings
          )
        )

      copy.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(ApiVersion.V7, baseSourceSettings, indexName).futureValue should contain allElementsOf Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "Typed Elasticsearch connector" should {
    "consume and publish documents as specific type" in assertAllStagesStopped {
      val indexName = "sink2"

      val copy = ElasticsearchSource
        .typed[Book](
          constructEsParams("source", "_doc", ApiVersion.V7),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            settings = baseWriteSettings
          )
        )

      copy.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(ApiVersion.V7, baseSourceSettings, indexName).futureValue should contain allElementsOf Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "ElasticsearchFlow" should {
    "store documents and pass failed documents to downstream" in assertAllStagesStopped {
      val indexName = "sink3"

      val copy = ElasticsearchSource
        .typed[Book](
          constructEsParams("source", "_doc", ApiVersion.V7),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .via(
          ElasticsearchFlow.create[Book](
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            settings = baseWriteSettings
          )
        )
        .runWith(Sink.seq)

      // Assert no errors
      copy.futureValue.filter(!_.success) shouldBe empty
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(ApiVersion.V7, baseSourceSettings, indexName).futureValue.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "store properly formatted JSON from Strings" in assertAllStagesStopped {
      val indexName = "sink3-0"

      val write: Future[immutable.Seq[WriteResult[String, NotUsed]]] = Source(
        immutable.Seq(
          WriteMessage.createIndexMessage("1", s"""{"title": "Das Parfum"}"""),
          WriteMessage.createIndexMessage("2", s"""{"title": "Faust"}"""),
          WriteMessage.createIndexMessage("3", s"""{"title": "Die unendliche Geschichte"}""")
        )
      ).via(
          ElasticsearchFlow.create(
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            settings = baseWriteSettings,
            StringMessageWriter
          )
        )
        .runWith(Sink.seq)

      // Assert no errors
      write.futureValue.filter(!_.success) shouldBe empty
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(ApiVersion.V7, baseSourceSettings, indexName).futureValue.sorted shouldEqual Seq(
        "Das Parfum",
        "Die unendliche Geschichte",
        "Faust"
      )
    }

    "kafka-example - store documents and pass Responses with passThrough" in assertAllStagesStopped {

      // We're going to pretend we got messages from kafka.
      // After we've written them to Elastic, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book 1"), KafkaOffset(0)),
        KafkaMessage(Book("Book 2"), KafkaOffset(1)),
        KafkaMessage(Book("Book 3"), KafkaOffset(2))
      )

      var committedOffsets = Vector[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val indexName = "sink6"
      val kafkaToEs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title

          // Transform message so that we can write to elastic
          WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            settings = baseWriteSettings
          )
        )
        .map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        }
        .runWith(Sink.ignore)

      kafkaToEs.futureValue shouldBe Done

      flushAndRefresh(connectionSettings, indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2)
      readTitlesFrom(ApiVersion.V7, baseSourceSettings, indexName).futureValue.toList should contain allElementsOf messagesFromKafka
        .map(_.book.title)
    }

    "handle multiple types of operations correctly" in assertAllStagesStopped {
      val indexName = "sink8"
      val requests = List[WriteMessage[Book, NotUsed]](
        WriteMessage.createIndexMessage(id = "00001", source = Book("Book 1")),
        WriteMessage.createUpsertMessage(id = "00002", source = Book("Book 2")),
        WriteMessage.createUpsertMessage(id = "00003", source = Book("Book 3")),
        WriteMessage.createUpdateMessage(id = "00004", source = Book("Book 4")),
        WriteMessage.createCreateMessage(id = "00005", source = Book("Book 5")),
        WriteMessage.createDeleteMessage(id = "00002")
      )

      val writeResults = Source(requests)
        .via(
          ElasticsearchFlow.create[Book](
            constructEsParams(indexName, "_doc", ApiVersion.V7),
            baseWriteSettings
          )
        )
        .runWith(Sink.seq)

      val results = writeResults.futureValue
      results should have size requests.size
      // Assert no errors except a missing document for a update request
      val errorMessages = results.flatMap(_.errorReason)
      errorMessages should have size 1
      errorMessages.head shouldEqual "[_doc][00004]: document missing"
      flushAndRefresh(connectionSettings, indexName)

      // Assert docs in sink8/_doc
      val readBooks = ElasticsearchSource(
        constructEsParams(indexName, "_doc", ApiVersion.V7),
        """{"match_all": {}}""",
        baseSourceSettings
      ).map { message =>
          message.source
        }
        .runWith(Sink.seq)

      // Docs should contain both columns
      readBooks.futureValue.sortBy(_.fields("title").compactPrint) shouldEqual Seq(
        JsObject("title" -> JsString("Book 1")),
        JsObject("title" -> JsString("Book 3")),
        JsObject("title" -> JsString("Book 5"))
      )
    }

    "use indexName supplied in message if present" in assertAllStagesStopped {
      // Copy source/_doc to sink2/_doc through typed stream

      val customIndexName = "custom-index"

      val writeCustomIndex = ElasticsearchSource
        .typed[Book](
          constructEsParams("source", "_doc", ApiVersion.V7),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[Book] =>
          WriteMessage
            .createIndexMessage(message.id, message.source)
            .withIndexName(customIndexName) // Setting the index-name to use for this document
        }
        .runWith(
          ElasticsearchSink.create[Book](
            constructEsParams("this-is-not-the-index-we-are-using", "_doc", ApiVersion.V7),
            settings = baseWriteSettings
          )
        )

      writeCustomIndex.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, customIndexName)
      readTitlesFrom(ApiVersion.V7, baseSourceSettings, customIndexName).futureValue.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "ElasticsearchSource" should {
    "be able to use custom searchParams" in assertAllStagesStopped {
      import spray.json._
      import DefaultJsonProtocol._

      case class TestDoc(id: String, a: String, b: Option[String], c: String)

      implicit val formatVersionTestDoc: JsonFormat[TestDoc] = jsonFormat4(TestDoc)

      val indexName = "custom-search-params-test-scala"
      val typeName = "_doc"

      val docs = List(
        TestDoc("1", "a1", Some("b1"), "c1"),
        TestDoc("2", "a2", Some("b2"), "c2"),
        TestDoc("3", "a3", Some("b3"), "c3")
      )

      // insert new documents
      val writes = Source(docs)
        .map { doc =>
          WriteMessage.createIndexMessage(doc.id, doc)
        }
        .via(
          ElasticsearchFlow.create[TestDoc](
            constructEsParams(indexName, typeName, ApiVersion.V7),
            baseWriteSettings.withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      writes.futureValue.filter(!_.success) shouldBe 'empty
      flushAndRefresh(connectionSettings, indexName)

      // Search for docs and ask elastic to only return some fields

      val readWithSearchParameters = ElasticsearchSource
        .typed[TestDoc](
          constructEsParams(indexName, typeName, ApiVersion.V7),
          searchParams = Map(
            "query" -> """ {"match_all": {}} """,
            "_source" -> """ ["id", "a", "c"] """
          ),
          baseSourceSettings
        )
        .map { message =>
          message.source
        }
        .runWith(Sink.seq)

      assert(readWithSearchParameters.futureValue.toList.sortBy(_.id) == docs.map(_.copy(b = None)))

    }
  }
}
