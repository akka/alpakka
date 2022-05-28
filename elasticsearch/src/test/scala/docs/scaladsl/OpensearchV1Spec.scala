/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.elasticsearch.{
  ElasticsearchConnectionSettings,
  OpensearchApiVersion,
  OpensearchConnectionSettings,
  ReadResult,
  StringMessageWriter,
  WriteMessage,
  WriteResult
}
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchFlow, ElasticsearchSink, ElasticsearchSource}
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import spray.json.jsonReader

import scala.collection.immutable
import scala.concurrent.Future
import spray.json._

class OpensearchV1Spec extends ElasticsearchSpecBase with ElasticsearchSpecUtils {

  private val connectionSettings: ElasticsearchConnectionSettings = OpensearchConnectionSettings(
    "http://localhost:9203"
  )
  private val baseSourceSettings = OpensearchSourceSettings(connectionSettings).withApiVersion(OpensearchApiVersion.V1)
  private val baseWriteSettings = OpensearchWriteSettings(connectionSettings).withApiVersion(OpensearchApiVersion.V1)

  override protected def beforeAll() = {
    insertTestData(connectionSettings)
  }

  override def afterAll() = {
    val deleteRequest = HttpRequest(HttpMethods.DELETE)
      .withUri(Uri(connectionSettings.baseUrl).withPath(Path("/_all")))
    http.singleRequest(deleteRequest).futureValue

    TestKit.shutdownActorSystem(system)
  }

  "Un-typed Opensearch connector" should {
    "consume and publish Json documents" in {
      val indexName = "sink2"

      //#run-jsobject
      val copy = ElasticsearchSource
        .create(
          constructElasticsearchParams("source", "_doc", OpensearchApiVersion.V1),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[spray.json.JsObject] =>
          val book: Book = jsonReader[Book].read(message.source)
          WriteMessage.createIndexMessage(message.id, book)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
      //#run-jsobject

      copy.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue should contain allElementsOf Seq(
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

  "Typed Opensearch connector" should {
    "consume and publish documents as specific type" in {
      val indexName = "sink2"

      //#run-typed
      val copy = ElasticsearchSource
        .typed[Book](
          constructElasticsearchParams("source", "_doc", OpensearchApiVersion.V1),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
      //#run-typed

      copy.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue should contain allElementsOf Seq(
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
    "store documents and pass failed documents to downstream" in {
      val indexName = "sink3"
      //#run-flow
      val copy = ElasticsearchSource
        .typed[Book](
          constructElasticsearchParams("source", "_doc", OpensearchApiVersion.V1),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .via(
          ElasticsearchFlow.create[Book](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      // Assert no errors
      copy.futureValue.filter(!_.success) shouldBe empty
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "store properly formatted JSON from Strings" in {
      val indexName = "sink3-0"

      // #string
      val write: Future[immutable.Seq[WriteResult[String, NotUsed]]] = Source(
        immutable.Seq(
          WriteMessage.createIndexMessage("1", Book("Das Parfum").toJson.toString()),
          WriteMessage.createIndexMessage("2", Book("Faust").toJson.toString()),
          WriteMessage.createIndexMessage("3", Book("Die unendliche Geschichte").toJson.toString())
        )
      ).via(
          ElasticsearchFlow.create(
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings,
            StringMessageWriter
          )
        )
        .runWith(Sink.seq)
      // #string

      // Assert no errors
      write.futureValue.filter(!_.success) shouldBe empty
      flushAndRefresh(connectionSettings, indexName)

      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.sorted shouldEqual Seq(
        "Das Parfum",
        "Die unendliche Geschichte",
        "Faust"
      )
    }

    "kafka-example - store documents and pass Responses with passThrough" in {

      //#kafka-example
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
      val kafkaToOs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title

          // Transform message so that we can write to elastic
          WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
        .map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        }
        .runWith(Sink.ignore)

      kafkaToOs.futureValue shouldBe Done
      //#kafka-example
      flushAndRefresh(connectionSettings, indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2)
      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.toList should contain allElementsOf messagesFromKafka
        .map(_.book.title)
    }

    "kafka-example - store documents and pass Responses with passThrough in bulk" in {

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

      val indexName = "sink6-bulk"
      val kafkaToOs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title

          // Transform message so that we can write to elastic
          WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .grouped(2)
        .via( // write to elastic
          ElasticsearchFlow.createBulk[Book, KafkaOffset](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
        .map(_.map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        })
        .runWith(Sink.ignore)

      kafkaToOs.futureValue shouldBe Done

      flushAndRefresh(connectionSettings, indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2)
      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.toList should contain allElementsOf messagesFromKafka
        .map(_.book.title)
    }

    "kafka-example - store documents and pass Responses with passThrough skipping some w/ NOP" in {

      // We're going to pretend we got messages from kafka.
      // After we've written them to Elastic, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book A", shouldSkip = Some(true)), KafkaOffset(0)),
        KafkaMessage(Book("Book 1"), KafkaOffset(1)),
        KafkaMessage(Book("Book 2"), KafkaOffset(2)),
        KafkaMessage(Book("Book B", shouldSkip = Some(true)), KafkaOffset(3)),
        KafkaMessage(Book("Book 3"), KafkaOffset(4)),
        KafkaMessage(Book("Book C", shouldSkip = Some(true)), KafkaOffset(5))
      )

      var committedOffsets = Vector[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val indexName = "sink6-nop"
      val kafkaToOs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title

          // Transform message so that we can write to elastic
          if (book.shouldSkip.getOrElse(false))
            WriteMessage.createNopMessage[Book]().withPassThrough(kafkaMessage.offset)
          else
            WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
        .map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        }
        .runWith(Sink.ignore)

      kafkaToOs.futureValue shouldBe Done

      flushAndRefresh(connectionSettings, indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2, 3, 4, 5)
      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.toList should contain allElementsOf messagesFromKafka
        .filterNot(_.book.shouldSkip.getOrElse(false))
        .map(_.book.title)
    }

    "kafka-example - skip all NOP documents and pass Responses with passThrough" in {

      // We're going to pretend we got messages from kafka.
      // After we've written them to Elastic, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book 1", shouldSkip = Some(true)), KafkaOffset(0)),
        KafkaMessage(Book("Book 2", shouldSkip = Some(true)), KafkaOffset(1)),
        KafkaMessage(Book("Book 3", shouldSkip = Some(true)), KafkaOffset(2))
      )

      var committedOffsets = Vector[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val indexName = "sink6-none"
      register(connectionSettings, indexName, "dummy", 10) // need to create index else exception in reading below

      val kafkaToOs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title

          // Transform message so that we can write to elastic
          if (book.shouldSkip.getOrElse(false))
            WriteMessage.createNopMessage[Book]().withPassThrough(kafkaMessage.offset)
          else
            WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
        .map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        }
        .runWith(Sink.ignore)

      kafkaToOs.futureValue shouldBe Done

      flushAndRefresh(connectionSettings, indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2)
      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, indexName).futureValue.toList shouldBe List("dummy")
    }

    "handle multiple types of operations correctly" in {
      val indexName = "sink8"
      //#multiple-operations
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
            constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
            baseWriteSettings
          )
        )
        .runWith(Sink.seq)
      //#multiple-operations

      val results = writeResults.futureValue
      results should have size requests.size
      // Assert no errors except a missing document for a update request
      val errorMessages = results.flatMap(_.errorReason)
      errorMessages should have size 1
      errorMessages.head shouldEqual "[_doc][00004]: document missing"
      flushAndRefresh(connectionSettings, indexName)

      // Assert docs in sink8/_doc
      val readBooks = ElasticsearchSource(
        constructElasticsearchParams(indexName, "_doc", OpensearchApiVersion.V1),
        """{"match_all": {}}""",
        baseSourceSettings
      ).map { message =>
          message.source
        }
        .runWith(Sink.seq)

      // Docs should contain both columns
      readBooks.futureValue.sortBy(_.fields("title").compactPrint) shouldEqual Seq(
        Book("Book 1").toJson,
        Book("Book 3").toJson,
        Book("Book 5").toJson
      )
    }

    "use indexName supplied in message if present" in {
      // Copy source/_doc to sink2/_doc through typed stream

      //#custom-index-name-example
      val customIndexName = "custom-index"

      val writeCustomIndex = ElasticsearchSource
        .typed[Book](
          constructElasticsearchParams("source", "_doc", OpensearchApiVersion.V1),
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
            constructElasticsearchParams("this-is-not-the-index-we-are-using", "_doc", OpensearchApiVersion.V1),
            settings = baseWriteSettings
          )
        )
      //#custom-index-name-example

      writeCustomIndex.futureValue shouldBe Done
      flushAndRefresh(connectionSettings, customIndexName)
      readTitlesFrom(OpensearchApiVersion.V1, baseSourceSettings, customIndexName).futureValue.sorted shouldEqual Seq(
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
    "be able to use custom searchParams" in {
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
            constructElasticsearchParams(indexName, typeName, OpensearchApiVersion.V1),
            baseWriteSettings.withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      writes.futureValue.filter(!_.success) shouldBe empty
      flushAndRefresh(connectionSettings, indexName)

      //#custom-search-params
      // Search for docs and ask elastic to only return some fields

      val readWithSearchParameters = ElasticsearchSource
        .typed[TestDoc](
          constructElasticsearchParams(indexName, typeName, OpensearchApiVersion.V1),
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
      //#custom-search-params

      assert(readWithSearchParameters.futureValue.toList.sortBy(_.id) == docs.map(_.copy(b = None)))

    }
  }
}
