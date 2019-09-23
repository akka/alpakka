/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.Collections
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class ElasticsearchSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with Inspectors {

  private implicit val patience = PatienceConfig(10.seconds)

  private val runner = new ElasticsearchClusterRunner()

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.http.HttpHost
  import org.elasticsearch.client.RestClient

  implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9201)).build()
  //#init-client

  //#define-class
  import spray.json._
  import DefaultJsonProtocol._

  case class Book(title: String)

  implicit val format: JsonFormat[Book] = jsonFormat1(Book)
  //#define-class

  override def beforeAll() = {
    runner.build(
      ElasticsearchClusterRunner
        .newConfigs()
        .baseHttpPort(9200)
        .baseTransportPort(9300)
        .numOfNode(1)
        .disableESLogger()
    )
    runner.ensureYellow()

    def register(indexName: String, title: String): Unit =
      client.performRequest("POST",
                            s"$indexName/_doc",
                            Map[String, String]().asJava,
                            new StringEntity(s"""{"title": "$title"}"""),
                            new BasicHeader("Content-Type", "application/json"))

    register("source", "Akka in Action")
    register("source", "Programming in Scala")
    register("source", "Learning Scala")
    register("source", "Scala for Spark in Production")
    register("source", "Scala Puzzlers")
    register("source", "Effective Akka")
    register("source", "Akka Concurrency")
    flush("source")
  }

  override def afterAll() = {
    runner.close()
    runner.clean()
    client.close()
    TestKit.shutdownActorSystem(system)
  }

  private def flush(indexName: String): Unit =
    client.performRequest("POST", s"$indexName/_flush")

  private def createStrictMapping(indexName: String): Unit =
    client.performRequest(
      "PUT",
      indexName,
      Collections.emptyMap[String, String],
      new StringEntity(s"""{
           |  "mappings": {
           |    "_doc": {
           |      "dynamic": "strict",
           |      "properties": {
           |        "title": { "type": "text"}
           |      }
           |    }
           |  }
           |}
         """.stripMargin),
      new BasicHeader("Content-Type", "application/json")
    )

  def documentation(): Unit = {
    //#source-settings
    val sourceSettings = ElasticsearchSourceSettings()
      .withBufferSize(10)
      .withScrollDuration(5.minutes)
    //#source-settings
    sourceSettings.toString should startWith("ElasticsearchSourceSettings(")
    //#sink-settings
    val sinkSettings =
      ElasticsearchWriteSettings()
        .withBufferSize(10)
        .withVersionType("internal")
        .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
    //#sink-settings
    sinkSettings.toString should startWith("ElasticsearchWriteSettings(")
  }

  private def readTitlesFrom(indexName: String): Future[immutable.Seq[String]] =
    ElasticsearchSource
      .typed[Book](
        indexName,
        "_doc",
        """{"match_all": {}}"""
      )
      .map { message =>
        message.source.title
      }
      .runWith(Sink.seq)

  "Source Settings" should {
    "convert scrollDuration value to correct scroll string value (Days)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.DAYS))

      sourceSettings.scroll shouldEqual "5d"
    }
    "convert scrollDuration value to correct scroll string value (Hours)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.HOURS))

      sourceSettings.scroll shouldEqual "5h"
    }
    "convert scrollDuration value to correct scroll string value (Minutes)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.MINUTES))

      sourceSettings.scroll shouldEqual "5m"
    }
    "convert scrollDuration value to correct scroll string value (Seconds)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.SECONDS))

      sourceSettings.scroll shouldEqual "5s"
    }
    "convert scrollDuration value to correct scroll string value (Milliseconds)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.MILLISECONDS))

      sourceSettings.scroll shouldEqual "5ms"
    }
    "convert scrollDuration value to correct scroll string value (Microseconds)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.MICROSECONDS))

      sourceSettings.scroll shouldEqual "5micros"
    }
    "convert scrollDuration value to correct scroll string value (Nanoseconds)" in {
      val sourceSettings = ElasticsearchSourceSettings()
        .withScrollDuration(FiniteDuration(5, TimeUnit.NANOSECONDS))

      sourceSettings.scroll shouldEqual "5nanos"
    }
  }

  "Un-typed Elasticsearch connector" should {
    "consume and publish Json documents" in assertAllStagesStopped {
      val indexName = "sink2"
      //#run-jsobject
      val copy = ElasticsearchSource
        .create(
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: ReadResult[spray.json.JsObject] =>
          val book: Book = jsonReader[Book].read(message.source)
          WriteMessage.createIndexMessage(message.id, book)
        }
        .runWith(
          ElasticsearchSink.create[Book, NotUsed](
            indexName,
            typeName = "_doc"
          )
        )
      //#run-jsobject

      copy.futureValue shouldBe Done
      flush(indexName)

      readTitlesFrom(indexName).futureValue should contain allElementsOf Seq(
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
      //#run-typed
      val copy = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .runWith(
          ElasticsearchSink.create[Book, NotUsed](
            indexName,
            typeName = "_doc"
          )
        )
      //#run-typed

      copy.futureValue shouldBe Done
      flush(indexName)

      readTitlesFrom(indexName).futureValue should contain allElementsOf Seq(
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
      //#run-flow
      val copy = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: ReadResult[Book] =>
          WriteMessage.createIndexMessage(message.id, message.source)
        }
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName = indexName,
            typeName = "_doc"
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      // Assert no errors
      copy.futureValue.filter(!_.success) shouldBe empty
      flush(indexName)

      readTitlesFrom(indexName).futureValue.sorted shouldEqual Seq(
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
      // #string
      val write: Future[immutable.Seq[WriteResult[String, NotUsed]]] = Source(
        immutable.Seq(
          WriteMessage.createIndexMessage("1", s"""{"title": "Das Parfum"}"""),
          WriteMessage.createIndexMessage("2", s"""{"title": "Faust"}"""),
          WriteMessage.createIndexMessage("3", s"""{"title": "Die unendliche Geschichte"}""")
        )
      ).via(
          ElasticsearchFlow.create(
            indexName = indexName,
            typeName = "_doc",
            ElasticsearchWriteSettings.Default,
            StringMessageWriter
          )
        )
        .runWith(Sink.seq)
      // #string

      // Assert no errors
      write.futureValue.filter(!_.success) shouldBe empty
      flush(indexName)

      readTitlesFrom(indexName).futureValue.sorted shouldEqual Seq(
        "Das Parfum",
        "Die unendliche Geschichte",
        "Faust"
      )
    }

    "pass through data in `withContext`" in assertAllStagesStopped {
      val books = immutable.Seq(
        "Akka in Action",
        "Alpakka Patterns"
      )

      val indexName = "sink3-1"
      val createBooks = Source(books).zipWithIndex
        .map {
          case (book, index) =>
            (WriteMessage.createIndexMessage(index.toString, Book(book)), book)
        }
        .via(
          ElasticsearchFlow.createWithContext(indexName, "_doc")
        )
        .runWith(Sink.seq)

      forAll(createBooks.futureValue) {
        case (writeMessage, title) =>
          val book = writeMessage.message.source
          book.map(_.title) should contain(title)
      }
    }

    "fail on using `withContext` with retries" in assertAllStagesStopped {
      intercept[IllegalArgumentException] {
        ElasticsearchFlow.createWithContext[Book, String, NotUsed](
          "shouldFail",
          "_doc",
          ElasticsearchWriteSettings().withRetryLogic(RetryAtFixedRate(2, 1.seconds))
        )
      }
    }

    "not post invalid encoded JSON" in assertAllStagesStopped {
      val books = immutable.Seq(
        "Akka in Action",
        "Akka \u00DF Concurrency"
      )

      val indexName = "sink4"
      val createBooks = Source(books.zipWithIndex)
        .map {
          case (book: String, index: Int) =>
            WriteMessage.createIndexMessage(index.toString, Book(book))
        }
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName,
            "_doc"
          )
        )
        .runWith(Sink.seq)

      // Assert no error
      createBooks.futureValue.filter(!_.success) shouldBe empty
      flush(indexName)
      readTitlesFrom(indexName).futureValue should contain allElementsOf Seq(
        "Akka in Action",
        "Akka \u00DF Concurrency"
      )
    }

    "retry a failed document and pass retried documents to downstream" in assertAllStagesStopped {
      val indexName = "sink5"

      // Create strict mapping to prevent invalid documents
      createStrictMapping(indexName)

      val createBooks = Source(
        immutable
          .Seq(
            Map("title" -> "Akka in Action").toJson,
            Map("subject" -> "Akka Concurrency").toJson
          )
          .zipWithIndex
      ).map {
          case (book: JsObject, index: Int) =>
            WriteMessage.createIndexMessage(index.toString, book)
          case _ => ??? // Keep the compiler from complaining
        }
        .via(
          ElasticsearchFlow.create(
            indexName,
            "_doc",
            ElasticsearchWriteSettings()
              .withRetryLogic(RetryAtFixedRate(5, 100.millis))
          )
        )
        .runWith(Sink.seq)

      val start = System.currentTimeMillis()
      val writeResults = createBooks.futureValue
      val end = System.currentTimeMillis()

      writeResults should have size 2

      // Assert retired documents
      val failed = writeResults.filter(!_.success).head
      failed.message shouldBe WriteMessage.createIndexMessage("1", Map("subject" -> "Akka Concurrency").toJson)
      failed.errorReason shouldBe Some(
        "mapping set to strict, dynamic introduction of [subject] within [_doc] is not allowed"
      )

      // Assert retried 5 times by looking duration
      assert(end - start > 5 * 100)

      flush(indexName)
      readTitlesFrom(indexName).futureValue shouldEqual Seq(
        "Akka in Action"
      )
    }

    "kafka-example - store documents and pass Responses with passThrough" in assertAllStagesStopped {

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
      val kafkaToEs = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title
          println("title: " + book.title)

          // Transform message so that we can write to elastic
          WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            indexName = indexName,
            typeName = "_doc"
          )
        )
        .map { result =>
          if (!result.success) throw new Exception("Failed to write message to elastic")
          // Commit to kafka
          commitToKafka(result.message.passThrough)
        }
        .runWith(Sink.ignore)

      kafkaToEs.futureValue shouldBe Done
      //#kafka-example
      flush(indexName)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset) should contain theSameElementsAs Seq(0, 1, 2)
      readTitlesFrom(indexName).futureValue.toList should contain allElementsOf messagesFromKafka.map(_.book.title)
    }

    "store new documents using upsert method and partially update existing ones" in assertAllStagesStopped {
      val books = List(
        ("00001", Book("Book 1")),
        ("00002", Book("Book 2")),
        ("00003", Book("Book 3"))
      )

      val indexName = "sink7"
      val createBooks = Source(books)
        .map { book: (String, Book) =>
          WriteMessage.createUpsertMessage(id = book._1, source = book._2)
        }
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName,
            "_doc"
          )
        )
        .runWith(Sink.seq)

      // Assert no errors
      createBooks.futureValue.filter(!_.success) shouldBe 'empty
      flush(indexName)

      // Create a second dataset with matching indexes to test partial update
      val updatedBooks = List(
        ("00001",
         JsObject(
           "rating" -> JsNumber(4)
         )),
        ("00002",
         JsObject(
           "rating" -> JsNumber(3)
         )),
        ("00003",
         JsObject(
           "rating" -> JsNumber(3)
         ))
      )

      // Update sink7/_doc with the second dataset
      val upserts = Source(updatedBooks)
        .map { book: (String, JsObject) =>
          WriteMessage.createUpsertMessage(id = book._1, source = book._2)
        }
        .via(
          ElasticsearchFlow.create[JsObject, NotUsed](
            indexName,
            "_doc"
          )
        )
        .runWith(Sink.seq)

      // Assert no errors
      upserts.futureValue.filter(!_.success) shouldBe 'empty
      flush(indexName)

      // Assert docs in sink7/_doc
      val readBooks = ElasticsearchSource(
        indexName,
        "_doc",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings()
      ).map { message =>
          message.source
        }
        .runWith(Sink.seq)

      // Docs should contain both columns
      readBooks.futureValue.sortBy(_.fields("title").compactPrint) shouldEqual Seq(
        JsObject(
          "title" -> JsString("Book 1"),
          "rating" -> JsNumber(4)
        ),
        JsObject(
          "title" -> JsString("Book 2"),
          "rating" -> JsNumber(3)
        ),
        JsObject(
          "title" -> JsString("Book 3"),
          "rating" -> JsNumber(3)
        )
      )
    }

    "handle multiple types of operations correctly" in assertAllStagesStopped {
      val indexName = "sink8"
      //#multiple-operations
      val requests = List[WriteMessage[Book, NotUsed, NotUsed]](
        WriteMessage.createIndexMessage(id = "00001", source = Book("Book 1")),
        WriteMessage.createUpsertMessage(id = "00002", source = Book("Book 2")),
        WriteMessage.createUpsertMessage(id = "00003", source = Book("Book 3")),
        WriteMessage.createUpdateMessage(id = "00004", source = Book("Book 4")),
        WriteMessage.createCreateMessage(id = "00005", source = Book("Book 5")),
        WriteMessage.createDeleteMessage(id = "00002")
      )

      val writeResults = Source(requests)
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName,
            "_doc"
          )
        )
        .runWith(Sink.seq)
      //#multiple-operations

      val results = writeResults.futureValue
      results should have size requests.size
      // Assert no errors except a missing document for a update request
      val errorMessages = results.flatMap(_.errorReason)
      errorMessages should have size 1
      errorMessages(0) shouldEqual "[_doc][00004]: document missing"
      flush(indexName)

      // Assert docs in sink8/_doc
      val readBooks = ElasticsearchSource(
        indexName,
        "_doc",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings()
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

    "Create existing document should fail" in {
      val indexName = "sink9"
      val requests = List[WriteMessage[Book, NotUsed]](
        WriteMessage.createIndexMessage(id = "00001", source = Book("Book 1")),
        WriteMessage.createCreateMessage(id = "00001", source = Book("Book 1"))
      )

      val writeResults = Source(requests)
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName,
            "_doc"
          )
        )
        .runWith(Sink.seq)

      val results = writeResults.futureValue
      results should have size requests.size
      // Assert error
      val errorMessages = results.flatMap(_.errorReason)
      errorMessages should have size 1
      errorMessages(0) shouldEqual "[_doc][00001]: version conflict, document already exists (current version [1])"
    }

    "read and write document-version if configured to do so" in assertAllStagesStopped {

      case class VersionTestDoc(id: String, name: String, value: Int)
      implicit val formatVersionTestDoc: JsonFormat[VersionTestDoc] = jsonFormat3(VersionTestDoc)

      val indexName = "version-test-scala"
      val typeName = "_doc"

      val docs = List(
        VersionTestDoc("1", "a", 0),
        VersionTestDoc("2", "b", 0),
        VersionTestDoc("3", "c", 0)
      )

      // insert new documents
      val indexResults = Source(docs)
        .map { doc =>
          WriteMessage.createIndexMessage(doc.id, doc)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchWriteSettings().withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      // Assert no errors
      indexResults.futureValue.filter(!_.success) shouldBe 'empty
      flush(indexName)

      // search for the documents and assert them being at version 1,
      // then update while specifying that for which version

      val updatedVersions = ElasticsearchSource
        .typed[VersionTestDoc](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings().withIncludeDocumentVersion(true)
        )
        .map { message =>
          val doc = message.source
          val version = message.version.get
          assert(1 == version) // Assert document got version = 1

          // Update it

          val newDoc = doc.copy(value = doc.value + 1)

          WriteMessage.createIndexMessage(newDoc.id, newDoc).withVersion(version)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchWriteSettings().withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      updatedVersions.futureValue.filter(!_.success) shouldBe 'empty

      flush(indexName)
      // Search again to assert that all documents are now on version 2
      val assertVersions = ElasticsearchSource
        .typed[VersionTestDoc](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings().withIncludeDocumentVersion(true)
        )
        .map { message =>
          val doc = message.source
          val version = message.version.get
          assert(doc.value == 1)
          assert(2 == version) // Assert document got version = 2
          doc
        }
        .runWith(Sink.ignore)

      assertVersions.futureValue shouldBe Done

      // Try to write document with old version - it should fail
      val illegalIndexWrites = Source
        .single(VersionTestDoc("1", "a", 2))
        .map { doc =>
          val oldVersion = 1
          WriteMessage.createIndexMessage(doc.id, doc).withVersion(oldVersion)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchWriteSettings().withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      val result5 = illegalIndexWrites.futureValue
      result5.head.success shouldBe false
    }

    "allow read and write using configured version type" in assertAllStagesStopped {

      val indexName = "book-test-version-type"
      val typeName = "_doc"

      val book = Book("A sample title")
      val docId = "1"
      val externalVersion = 5L

      // Insert new document using external version
      val insertWrite = Source
        .single(book)
        .map { doc =>
          WriteMessage.createIndexMessage(docId, doc).withVersion(externalVersion)
        }
        .via(
          ElasticsearchFlow.create[Book, NotUsed](
            indexName,
            typeName,
            ElasticsearchWriteSettings().withBufferSize(5).withVersionType("external")
          )
        )
        .runWith(Sink.seq)

      val insertResult = insertWrite.futureValue.head
      assert(insertResult.success)

      flush(indexName)

      // Assert that the document's external version is saved
      val readFirst = ElasticsearchSource
        .typed[Book](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings().withIncludeDocumentVersion(true)
        )
        .runWith(Sink.head)

      assert(readFirst.futureValue.version.contains(externalVersion))
    }

    "use indexName supplied in message if present" in assertAllStagesStopped {
      // Copy source/_doc to sink2/_doc through typed stream

      //#custom-index-name-example
      val customIndexName = "custom-index"

      val writeCustomIndex = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: ReadResult[Book] =>
          WriteMessage
            .createIndexMessage(message.id, message.source)
            .withIndexName(customIndexName) // Setting the index-name to use for this document
        }
        .runWith(
          ElasticsearchSink.create[Book, NotUsed](
            indexName = "this-is-not-the-index-we-are-using",
            typeName = "_doc"
          )
        )
      //#custom-index-name-example

      writeCustomIndex.futureValue shouldBe Done
      flush(customIndexName)
      readTitlesFrom(customIndexName).futureValue.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "throw an IllegalArgumentException if indexName is null when passed to ElasticsearchSource" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchSource
          .create(
            indexName = null,
            typeName = "_doc",
            query = """{"match_all": {}}"""
          )
      }
    }

    "throw an IllegalArgumentException if indexName is null when passed to ElasticsearchFlow" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchFlow
          .create[Book, NotUsed](
            indexName = null,
            typeName = "_doc"
          )
      }
    }

    "throw an IllegalArgumentException if typeName is null when passed to ElasticsearchFlow" in {
      assertThrows[IllegalArgumentException] {
        ElasticsearchFlow
          .create[Book](
            indexName = "foo",
            typeName = null
          )
      }
    }
  }

  "ElasticsearchSource" should {
    "allow search without specifying typeName" in assertAllStagesStopped {
      val readWithoutTypeName = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = None,
          query = """{"match_all": {}}""",
          settings = ElasticsearchSourceSettings().withBufferSize(5)
        )
        .map(_.source.title)
        .runWith(Sink.seq)

      val result = readWithoutTypeName.futureValue.toList
      result.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "be able to use custom searchParams" in assertAllStagesStopped {

      //#custom-search-params
      case class TestDoc(id: String, a: String, b: Option[String], c: String)
      //#custom-search-params

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
          ElasticsearchFlow.create[TestDoc, NotUsed](
            indexName,
            typeName,
            ElasticsearchWriteSettings().withBufferSize(5)
          )
        )
        .runWith(Sink.seq)

      writes.futureValue.filter(!_.success) shouldBe 'empty
      flush(indexName)

      //#custom-search-params
      // Search for docs and ask elastic to only return some fields

      val readWithSearchParameters = ElasticsearchSource
        .typed[TestDoc](indexName,
                        Some(typeName),
                        searchParams = Map(
                          "query" -> """ {"match_all": {}} """,
                          "_source" -> """ ["id", "a", "c"] """
                        ),
                        ElasticsearchSourceSettings())
        .map { message =>
          message.source
        }
        .runWith(Sink.seq)

      //#custom-search-params

      assert(readWithSearchParameters.futureValue.toList.sortBy(_.id) == docs.map(_.copy(b = None)))

    }
  }

  def compileOnlySample(): Unit = {
    val doc = "dummy-doc"
    //#custom-metadata-example
    val msg = WriteMessage
      .createIndexMessage(doc)
      .withCustomMetadata(Map("pipeline" -> "myPipeline"))
    //#custom-metadata-example
    msg.customMetadata should contain("pipeline")
  }

}
