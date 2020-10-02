/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, Uri}
import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.{Done, NotUsed}
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

trait ElasticsearchConnectorBehaviour { this: AnyWordSpec with Matchers with ScalaFutures with Inspectors =>

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(10.seconds)

  def elasticsearchConnector(apiVersion: ApiVersion, connectionSettings: ElasticsearchConnectionSettings)(
      implicit materializer: Materializer,
      http: HttpExt
  ): Unit = {

    val baseSourceSettings = ElasticsearchSourceSettings().withApiVersion(apiVersion).withConnection(connectionSettings)
    val baseWriteSettings = ElasticsearchWriteSettings().withApiVersion(apiVersion).withConnection(connectionSettings)

    //#define-class
    import spray.json._
    import DefaultJsonProtocol._

    case class Book(title: String)

    implicit val format: JsonFormat[Book] = jsonFormat1(Book)
    //#define-class

    def register(indexName: String, title: String): Unit = {
      val request = HttpRequest(HttpMethods.POST)
        .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_doc")))
        .withEntity(ContentTypes.`application/json`, s"""{"title": "$title"}""")
      http.singleRequest(request).futureValue
    }

    def flushAndRefresh(indexName: String): Unit = {
      val flushRequest = HttpRequest(HttpMethods.POST)
        .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_flush")))
      http.singleRequest(flushRequest).futureValue

      val refreshRequest = HttpRequest(HttpMethods.POST)
        .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_refresh")))
      http.singleRequest(refreshRequest).futureValue
    }

    def createStrictMapping(indexName: String): Unit = {
      val uri = Uri(connectionSettings.baseUrl)
        .withPath(Path(s"/$indexName"))
        .withQuery(Uri.Query(Map("include_type_name" -> "true")))

      val request = HttpRequest(HttpMethods.PUT)
        .withUri(uri)
        .withEntity(
          ContentTypes.`application/json`,
          s"""{
            |  "mappings": {
            |    "_doc": {
            |      "dynamic": "strict",
            |      "properties": {
            |        "title": { "type": "text"}
            |      }
            |    }
            |  }
            |}
         """.stripMargin
        )

      http.singleRequest(request).futureValue
    }

    def readTitlesFrom(indexName: String): Future[immutable.Seq[String]] =
      ElasticsearchSource
        .typed[Book](
          constructEsParams(indexName, "_doc", apiVersion),
          query = """{"match_all": {}}""",
          settings = baseSourceSettings
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

    register("source", "Akka in Action")
    register("source", "Programming in Scala")
    register("source", "Learning Scala")
    register("source", "Scala for Spark in Production")
    register("source", "Scala Puzzlers")
    register("source", "Effective Akka")
    register("source", "Akka Concurrency")
    flushAndRefresh("source")

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

    "Sink Settings" should {
      "copy explicit index name permission" in {
        val sinkSettings =
          ElasticsearchWriteSettings()
            .withBufferSize(10)
            .withVersionType("internal")
            .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

        val restrictiveCopy = sinkSettings.withAllowExplicitIndex(false)

        restrictiveCopy.allowExplicitIndex shouldEqual false
      }
    }

    "Un-typed Elasticsearch connector" should {
      "consume and publish Json documents" in assertAllStagesStopped {
        val indexName = "sink2"
        //#run-jsobject
        val copy = ElasticsearchSource
          .create(
            constructEsParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings
          )
          .map { message: ReadResult[spray.json.JsObject] =>
            val book: Book = jsonReader[Book].read(message.source)
            WriteMessage.createIndexMessage(message.id, book)
          }
          .runWith(
            ElasticsearchSink.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              settings = baseWriteSettings
            )
          )
        //#run-jsobject

        copy.futureValue shouldBe Done
        flushAndRefresh(indexName)

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
            constructEsParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings
          )
          .map { message: ReadResult[Book] =>
            WriteMessage.createIndexMessage(message.id, message.source)
          }
          .runWith(
            ElasticsearchSink.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              settings = baseWriteSettings
            )
          )
        //#run-typed

        copy.futureValue shouldBe Done
        flushAndRefresh(indexName)

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
            constructEsParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings
          )
          .map { message: ReadResult[Book] =>
            WriteMessage.createIndexMessage(message.id, message.source)
          }
          .via(
            ElasticsearchFlow.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              settings = baseWriteSettings
            )
          )
          .runWith(Sink.seq)
        //#run-flow

        // Assert no errors
        copy.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(indexName)

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
              constructEsParams(indexName, "_doc", apiVersion),
              settings = baseWriteSettings,
              StringMessageWriter
            )
          )
          .runWith(Sink.seq)
        // #string

        // Assert no errors
        write.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(indexName)

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
            ElasticsearchFlow.createWithContext(
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        forAll(createBooks.futureValue) {
          case (writeMessage, title) =>
            val book = writeMessage.message.source
            book.map(_.title) should contain(title)
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
            ElasticsearchFlow.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no error
        createBooks.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(indexName)
        readTitlesFrom(indexName).futureValue should contain allElementsOf Seq(
          "Akka in Action",
          "Akka \u00DF Concurrency"
        )
      }

      "retry a failed document and pass retried documents to downstream (create)" in assertAllStagesStopped {
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
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 100.millis))
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

        flushAndRefresh(indexName)
        readTitlesFrom(indexName).futureValue shouldEqual Seq(
          "Akka in Action"
        )
      }

      "retry ALL failed document and pass retried documents to downstream (createWithPassThrough)" in assertAllStagesStopped {
        val indexName = "sink5_1"

        val bookNr = 100
        val writeMsgs = Iterator
          .from(0)
          .take(bookNr)
          .grouped(5)
          .zipWithIndex
          .flatMap {
            case (numBlock, index) =>
              val writeMsgBlock = numBlock.map { n =>
                WriteMessage
                  .createCreateMessage(n.toString, Map("title" -> s"Book ${n}"))
                  .withPassThrough(n)
              }

              val writeMsgFailed = WriteMessage
                .createCreateMessage("0", Map("title" -> s"Failed"))
                .withPassThrough(bookNr + index)

              (writeMsgBlock ++ Iterator(writeMsgFailed)).toList
          }
          .toList

        val createBooks = Source(writeMsgs)
          .via(
            ElasticsearchFlow.createWithPassThrough(
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 1.millis))
            )
          )
          .runWith(Sink.seq)

        val writeResults = createBooks.futureValue

        writeResults should have size writeMsgs.size

        flushAndRefresh(indexName)

        val expectedBookTitles = Iterator.from(0).map(n => s"Book ${n}").take(bookNr).toSet
        readTitlesFrom(indexName).futureValue should contain theSameElementsAs expectedBookTitles
      }

      "retry a failed document and pass retried documents to downstream (createWithContext)" in assertAllStagesStopped {
        val indexName = "sink5b"

        // Create strict mapping to prevent invalid documents
        createStrictMapping(indexName)

        val createBooks = Source(
          immutable
            .Seq(
              Map("title" -> "Akka in Action").toJson,
              Map("subject" -> "Akka Concurrency").toJson,
              Map("title" -> "Learning Scala").toJson
            )
            .zipWithIndex
        ).map {
            case (book: JsObject, index: Int) =>
              WriteMessage.createIndexMessage(index.toString, book) -> index
            case _ => ??? // Keep the compiler from complaining
          }
          .via(
            ElasticsearchFlow.createWithContext(
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 100.millis))
            )
          )
          .runWith(Sink.seq)

        val start = System.currentTimeMillis()
        val writeResults = createBooks.futureValue
        val end = System.currentTimeMillis()

        writeResults should have size 3

        // Assert retired documents
        writeResults.map(_._2) should contain theSameElementsInOrderAs Seq(0, 1, 2)

        val (failed, _) = writeResults.filter(!_._1.success).head
        failed.message shouldBe WriteMessage
          .createIndexMessage("1", Map("subject" -> "Akka Concurrency").toJson)
          .withPassThrough(1)
        failed.errorReason shouldBe Some(
          "mapping set to strict, dynamic introduction of [subject] within [_doc] is not allowed"
        )

        // Assert retried 5 times by looking duration
        assert(end - start > 5 * 100)

        flushAndRefresh(indexName)
        readTitlesFrom(indexName).futureValue should contain theSameElementsAs Seq(
          "Akka in Action",
          "Learning Scala"
        )
      }

      "retry ALL failed document and pass retried documents to downstream (createWithContext)" in assertAllStagesStopped {
        val indexName = "sink5_1b"

        val bookNr = 100
        val writeMsgs = Iterator
          .from(0)
          .take(bookNr)
          .grouped(5)
          .zipWithIndex
          .flatMap {
            case (numBlock, index) =>
              val writeMsgBlock = numBlock.map { n =>
                WriteMessage
                  .createCreateMessage(n.toString, Map("title" -> s"Book ${n}")) -> n
              }

              val writeMsgFailed = WriteMessage
                  .createCreateMessage("0", Map("title" -> s"Failed")) -> (bookNr + index)

              (writeMsgBlock ++ Iterator(writeMsgFailed)).toList
          }
          .toList

        val createBooks = Source(writeMsgs)
          .via(
            ElasticsearchFlow.createWithContext(
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 1.millis))
            )
          )
          .runWith(Sink.seq)

        val writeResults = createBooks.futureValue

        writeResults should have size writeMsgs.size
        writeResults.map(_._2) should contain theSameElementsInOrderAs writeMsgs.map(_._2)

        flushAndRefresh(indexName)

        val expectedBookTitles = Iterator.from(0).map(n => s"Book ${n}").take(bookNr).toSet
        readTitlesFrom(indexName).futureValue should contain theSameElementsAs expectedBookTitles
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

            // Transform message so that we can write to elastic
            WriteMessage.createIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
          }
          .via( // write to elastic
            ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
              constructEsParams(indexName, "_doc", apiVersion),
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
        //#kafka-example
        flushAndRefresh(indexName)

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
            ElasticsearchFlow.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        createBooks.futureValue.filter(!_.success) shouldBe 'empty
        flushAndRefresh(indexName)

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
            ElasticsearchFlow.create[JsObject](
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        upserts.futureValue.filter(!_.success) shouldBe 'empty
        flushAndRefresh(indexName)

        // Assert docs in sink7/_doc
        val readBooks = ElasticsearchSource(
          constructEsParams(indexName, "_doc", apiVersion),
          """{"match_all": {}}""",
          baseSourceSettings
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
              constructEsParams(indexName, "_doc", apiVersion),
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
        flushAndRefresh(indexName)

        // Assert docs in sink8/_doc
        val readBooks = ElasticsearchSource(
          constructEsParams(indexName, "_doc", apiVersion),
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

      "Create existing document should fail" in {
        val indexName = "sink9"
        val requests = List[WriteMessage[Book, NotUsed]](
          WriteMessage.createIndexMessage(id = "00001", source = Book("Book 1")),
          WriteMessage.createCreateMessage(id = "00001", source = Book("Book 1"))
        )

        val writeResults = Source(requests)
          .via(
            ElasticsearchFlow.create[Book](
              constructEsParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        val results = writeResults.futureValue
        results should have size requests.size
        // Assert error
        val errorMessages = results.flatMap(_.errorReason)
        errorMessages should have size 1
        errorMessages.head should include("version conflict, document already exists (current version [1])")
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
              constructEsParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5)
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        indexResults.futureValue.filter(!_.success) shouldBe 'empty
        flushAndRefresh(indexName)

        // search for the documents and assert them being at version 1,
        // then update while specifying that for which version

        val updatedVersions = ElasticsearchSource
          .typed[VersionTestDoc](
            constructEsParams(indexName, typeName, apiVersion),
            """{"match_all": {}}""",
            baseSourceSettings.withIncludeDocumentVersion(true)
          )
          .map { message =>
            val doc = message.source
            val version = message.version.get
            assert(1 == version) // Assert document got version = 1

            // Update it

            val newDoc = doc.copy(value = doc.value + 1)

            WriteMessage.createIndexMessage(newDoc.id, newDoc).withVersion(version + 1)
          }
          .via(
            ElasticsearchFlow.create[VersionTestDoc](
              constructEsParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
            )
          )
          .runWith(Sink.seq)

        updatedVersions.futureValue.filter(!_.success) shouldBe 'empty

        flushAndRefresh(indexName)
        // Search again to assert that all documents are now on version 2
        val assertVersions = ElasticsearchSource
          .typed[VersionTestDoc](
            constructEsParams(indexName, typeName, apiVersion),
            """{"match_all": {}}""",
            baseSourceSettings.withIncludeDocumentVersion(true)
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
              constructEsParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
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
            ElasticsearchFlow.create[Book](
              constructEsParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
            )
          )
          .runWith(Sink.seq)

        val insertResult = insertWrite.futureValue.head
        assert(insertResult.success)

        flushAndRefresh(indexName)

        // Assert that the document's external version is saved
        val readFirst = ElasticsearchSource
          .typed[Book](
            constructEsParams(indexName, typeName, apiVersion),
            """{"match_all": {}}""",
            settings = baseSourceSettings.withIncludeDocumentVersion(true)
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
            constructEsParams("source", "_doc", apiVersion),
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
              constructEsParams("this-is-not-the-index-we-are-using", "_doc", apiVersion),
              settings = baseWriteSettings
            )
          )
        //#custom-index-name-example

        writeCustomIndex.futureValue shouldBe Done
        flushAndRefresh(customIndexName)
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
    }

    "ElasticsearchSource" should {
      "allow search without specifying typeName" in assertAllStagesStopped {
        val readWithoutTypeName = ElasticsearchSource
          .typed[Book](
            constructEsParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings.withBufferSize(5).withApiVersion(ApiVersion.V7)
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
            ElasticsearchFlow.create[TestDoc](
              constructEsParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5)
            )
          )
          .runWith(Sink.seq)

        writes.futureValue.filter(!_.success) shouldBe 'empty
        flushAndRefresh(indexName)

        //#custom-search-params
        // Search for docs and ask elastic to only return some fields

        val readWithSearchParameters = ElasticsearchSource
          .typed[TestDoc](
            constructEsParams(indexName, typeName, apiVersion),
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

    lazy val _ = {
      //#connection-settings
      val connectionSettings = ElasticsearchConnectionSettings()
        .withBaseUrl("http://localhost:9200")
        .withCredentials("user", "password")
      //#connection-settings
      //#source-settings
      val sourceSettings = ElasticsearchSourceSettings()
        .withConnection(connectionSettings)
        .withBufferSize(10)
        .withScrollDuration(5.minutes)
      //#source-settings
      sourceSettings.toString should startWith("ElasticsearchSourceSettings(")
      //#sink-settings
      val sinkSettings =
        ElasticsearchWriteSettings()
          .withConnection(connectionSettings)
          .withBufferSize(10)
          .withVersionType("internal")
          .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
          .withApiVersion(ApiVersion.V5)
      //#sink-settings
      sinkSettings.toString should startWith("ElasticsearchWriteSettings(")
      //#es-params
      val esParamsV5 = EsParams.V5("index", "_doc")
      val esParamsV7 = EsParams.V7("index")
      //#es-params
      esParamsV5.toString should startWith("EsParams(")
      esParamsV7.toString should startWith("EsParams(")
      val doc = "dummy-doc"
      //#custom-metadata-example
      val msg = WriteMessage
        .createIndexMessage(doc)
        .withCustomMetadata(Map("pipeline" -> "myPipeline"))
      //#custom-metadata-example
      msg.customMetadata should contain("pipeline")
    }

  }

  private def constructEsParams(indexName: String, typeName: String, apiVersion: ApiVersion): EsParams = {
    if (apiVersion == ApiVersion.V5) {
      EsParams.V5(indexName, typeName)
    } else {
      EsParams.V7(indexName)
    }
  }

}
