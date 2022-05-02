/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.duration._

trait OpensearchConnectorBehaviour {
  this: AnyWordSpec with Matchers with ScalaFutures with Inspectors with ElasticsearchSpecUtils =>

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(10.seconds)

  def opensearchConnector(apiVersion: OpensearchApiVersion, connectionSettings: ElasticsearchConnectionSettings)(
      implicit system: ActorSystem,
      http: HttpExt
  ): Unit = {

    val baseSourceSettings = OpensearchSourceSettings(connectionSettings).withApiVersion(apiVersion)
    val baseWriteSettings = OpensearchWriteSettings(connectionSettings).withApiVersion(apiVersion)

    import spray.json._
    import DefaultJsonProtocol._

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
            |        "title": { "type": "text"},
            |        "price": { "type": "integer"}
            |      }
            |    }
            |  }
            |}
         """.stripMargin
        )

      http.singleRequest(request).futureValue
    }

    "Source Settings" should {
      "convert scrollDuration value to correct scroll string value (Days)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.DAYS))

        sourceSettings.scroll shouldEqual "5d"
      }
      "convert scrollDuration value to correct scroll string value (Hours)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.HOURS))

        sourceSettings.scroll shouldEqual "5h"
      }
      "convert scrollDuration value to correct scroll string value (Minutes)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.MINUTES))

        sourceSettings.scroll shouldEqual "5m"
      }
      "convert scrollDuration value to correct scroll string value (Seconds)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.SECONDS))

        sourceSettings.scroll shouldEqual "5s"
      }
      "convert scrollDuration value to correct scroll string value (Milliseconds)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.MILLISECONDS))

        sourceSettings.scroll shouldEqual "5ms"
      }
      "convert scrollDuration value to correct scroll string value (Microseconds)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.MICROSECONDS))

        sourceSettings.scroll shouldEqual "5micros"
      }
      "convert scrollDuration value to correct scroll string value (Nanoseconds)" in {
        val sourceSettings = OpensearchSourceSettings(connectionSettings)
          .withScrollDuration(FiniteDuration(5, TimeUnit.NANOSECONDS))

        sourceSettings.scroll shouldEqual "5nanos"
      }
    }

    "Sink Settings" should {
      "copy explicit index name permission" in {
        val sinkSettings =
          OpensearchWriteSettings(connectionSettings)
            .withBufferSize(10)
            .withVersionType("internal")
            .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

        val restrictiveCopy = sinkSettings.withAllowExplicitIndex(false)

        restrictiveCopy.allowExplicitIndex shouldEqual false
      }
    }

    "ElasticsearchFlow" should {
      "pass through data in `withContext`" in {
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
              constructElasticsearchParams(indexName, "_doc", apiVersion),
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

      "not post invalid encoded JSON" in {
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
              constructElasticsearchParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no error
        createBooks.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(connectionSettings, indexName)
        readTitlesFrom(apiVersion, baseSourceSettings, indexName).futureValue should contain allElementsOf Seq(
          "Akka in Action",
          "Akka \u00DF Concurrency"
        )
      }

      "retry a failed document and pass retried documents to downstream (create)" in {
        val indexName = "sink5"

        // Create strict mapping to prevent invalid documents
        createStrictMapping(indexName)

        val createBooks = Source(
          immutable
            .Seq(
              Book("Akka in Action").toJson,
              JsObject("subject" -> "Akka Concurrency".toJson)
            )
            .zipWithIndex
        ).map {
            case (book: JsObject, index: Int) =>
              WriteMessage.createIndexMessage(index.toString, book)
            case _ => ??? // Keep the compiler from complaining
          }
          .via(
            ElasticsearchFlow.create(
              constructElasticsearchParams(indexName, "_doc", apiVersion),
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
        failed.message shouldBe WriteMessage.createIndexMessage("1", JsObject("subject" -> "Akka Concurrency".toJson))
        failed.errorReason shouldBe Some(
          "mapping set to strict, dynamic introduction of [subject] within [_doc] is not allowed"
        )

        // Assert retried 5 times by looking duration
        assert(end - start > 5 * 100)

        flushAndRefresh(connectionSettings, indexName)
        readTitlesFrom(apiVersion, baseSourceSettings, indexName).futureValue shouldEqual Seq(
          "Akka in Action"
        )
      }

      "retry ALL failed document and pass retried documents to downstream (createWithPassThrough)" in {
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
                  .createCreateMessage(n.toString, Book(s"Book ${n}"))
                  .withPassThrough(n)
              }

              val writeMsgFailed = WriteMessage
                .createCreateMessage("0", Book(s"Failed"))
                .withPassThrough(bookNr + index)

              (writeMsgBlock ++ Iterator(writeMsgFailed)).toList
          }
          .toList

        val createBooks = Source(writeMsgs)
          .via(
            ElasticsearchFlow.createWithPassThrough(
              constructElasticsearchParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 1.millis))
            )
          )
          .runWith(Sink.seq)

        val writeResults = createBooks.futureValue

        writeResults should have size writeMsgs.size

        flushAndRefresh(connectionSettings, indexName)

        val expectedBookTitles = Iterator.from(0).map(n => s"Book ${n}").take(bookNr).toSet
        readTitlesFrom(apiVersion, baseSourceSettings, indexName).futureValue should contain theSameElementsAs expectedBookTitles
      }

      "retry a failed document and pass retried documents to downstream (createWithContext)" in {
        val indexName = "sink5b"

        // Create strict mapping to prevent invalid documents
        createStrictMapping(indexName)

        val createBooks = Source(
          immutable
            .Seq(
              Book("Akka in Action").toJson,
              JsObject("subject" -> "Akka Concurrency".toJson),
              Book("Learning Scala").toJson
            )
            .zipWithIndex
        ).map {
            case (book: JsObject, index: Int) =>
              WriteMessage.createIndexMessage(index.toString, book) -> index
            case _ => ??? // Keep the compiler from complaining
          }
          .via(
            ElasticsearchFlow.createWithContext(
              constructElasticsearchParams(indexName, "_doc", apiVersion),
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
          .createIndexMessage("1", JsObject("subject" -> "Akka Concurrency".toJson))
          .withPassThrough(1)
        failed.errorReason shouldBe Some(
          "mapping set to strict, dynamic introduction of [subject] within [_doc] is not allowed"
        )

        // Assert retried 5 times by looking duration
        assert(end - start > 5 * 100)

        flushAndRefresh(connectionSettings, indexName)
        readTitlesFrom(apiVersion, baseSourceSettings, indexName).futureValue should contain theSameElementsAs Seq(
          "Akka in Action",
          "Learning Scala"
        )
      }

      "retry ALL failed document and pass retried documents to downstream (createWithContext)" in {
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
                  .createCreateMessage(n.toString, Book(s"Book ${n}")) -> n
              }

              val writeMsgFailed = WriteMessage
                  .createCreateMessage("0", Book(s"Failed")) -> (bookNr + index)

              (writeMsgBlock ++ Iterator(writeMsgFailed)).toList
          }
          .toList

        val createBooks = Source(writeMsgs)
          .via(
            ElasticsearchFlow.createWithContext(
              constructElasticsearchParams(indexName, "_doc", apiVersion),
              baseWriteSettings.withRetryLogic(RetryAtFixedRate(5, 1.millis))
            )
          )
          .runWith(Sink.seq)

        val writeResults = createBooks.futureValue

        writeResults should have size writeMsgs.size
        writeResults.map(_._2) should contain theSameElementsInOrderAs writeMsgs.map(_._2)

        flushAndRefresh(connectionSettings, indexName)

        val expectedBookTitles = Iterator.from(0).map(n => s"Book ${n}").take(bookNr).toSet
        readTitlesFrom(apiVersion, baseSourceSettings, indexName).futureValue should contain theSameElementsAs expectedBookTitles
      }

      "store new documents using upsert method and partially update existing ones" in {
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
              constructElasticsearchParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        createBooks.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(connectionSettings, indexName)

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
              constructElasticsearchParams(indexName, "_doc", apiVersion),
              baseWriteSettings
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        upserts.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(connectionSettings, indexName)

        // Assert docs in sink7/_doc
        val readBooks = ElasticsearchSource(
          constructElasticsearchParams(indexName, "_doc", apiVersion),
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
            "rating" -> JsNumber(4),
            "price" -> JsNumber(10)
          ),
          JsObject(
            "title" -> JsString("Book 2"),
            "rating" -> JsNumber(3),
            "price" -> JsNumber(10)
          ),
          JsObject(
            "title" -> JsString("Book 3"),
            "rating" -> JsNumber(3),
            "price" -> JsNumber(10)
          )
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
              constructElasticsearchParams(indexName, "_doc", apiVersion),
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

      "read and write document-version if configured to do so" in {

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
              constructElasticsearchParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5)
            )
          )
          .runWith(Sink.seq)

        // Assert no errors
        indexResults.futureValue.filter(!_.success) shouldBe empty
        flushAndRefresh(connectionSettings, indexName)

        // search for the documents and assert them being at version 1,
        // then update while specifying that for which version

        val updatedVersions = ElasticsearchSource
          .typed[VersionTestDoc](
            constructElasticsearchParams(indexName, typeName, apiVersion),
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
              constructElasticsearchParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
            )
          )
          .runWith(Sink.seq)

        updatedVersions.futureValue.filter(!_.success) shouldBe empty

        flushAndRefresh(connectionSettings, indexName)
        // Search again to assert that all documents are now on version 2
        val assertVersions = ElasticsearchSource
          .typed[VersionTestDoc](
            constructElasticsearchParams(indexName, typeName, apiVersion),
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
              constructElasticsearchParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
            )
          )
          .runWith(Sink.seq)

        val result5 = illegalIndexWrites.futureValue
        result5.head.success shouldBe false
      }

      "allow read and write using configured version type" in {

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
              constructElasticsearchParams(indexName, typeName, apiVersion),
              baseWriteSettings.withBufferSize(5).withVersionType("external")
            )
          )
          .runWith(Sink.seq)

        val insertResult = insertWrite.futureValue.head
        assert(insertResult.success)

        flushAndRefresh(connectionSettings, indexName)

        // Assert that the document's external version is saved
        val readFirst = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams(indexName, typeName, apiVersion),
            """{"match_all": {}}""",
            settings = baseSourceSettings.withIncludeDocumentVersion(true)
          )
          .runWith(Sink.head)

        assert(readFirst.futureValue.version.contains(externalVersion))
      }
    }

    "ElasticsearchSource" should {
      insertTestData(connectionSettings)

      "allow search without specifying typeName" in {
        val readWithoutTypeName = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings.withBufferSize(5).withApiVersion(OpensearchApiVersion.V1)
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

      "allow search on index pattern with no matching index" in {
        val readWithoutTypeName = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams("missing-*", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings.withBufferSize(5)
          )
          .map(_.source.title)
          .runWith(Sink.seq)

        val result = readWithoutTypeName.futureValue.toList
        result shouldEqual Seq()
      }

      "sort by _doc by default" in {
        val read = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams("source", "_doc", apiVersion),
            query = """{"match_all": {}}""",
            settings = baseSourceSettings.withBufferSize(3).withApiVersion(apiVersion)
          )
          .map(_.source.title)
          .runWith(Sink.seq)

        val result = read.futureValue.toList

        // sort: _doc is by design an undefined order and is non-deterministic
        // we cannot check a specific order of values
        result should contain theSameElementsAs (List("Akka in Action",
                                                      "Programming in Scala",
                                                      "Learning Scala",
                                                      "Scala for Spark in Production",
                                                      "Scala Puzzlers",
                                                      "Effective Akka",
                                                      "Akka Concurrency"))
      }

      "sort by user defined field" in {
        val read = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams("source", "_doc", apiVersion),
            Map(
              "query" -> """{"match_all": {}}""",
              "sort" -> """["price"]"""
            ),
            settings = baseSourceSettings.withBufferSize(3).withApiVersion(apiVersion)
          )
          .map(_.source.price)
          .runWith(Sink.seq)

        val result = read.futureValue.toList
        result shouldEqual result.sorted

      }

      "sort by user defined field desc" in {
        val read = ElasticsearchSource
          .typed[Book](
            constructElasticsearchParams("source", "_doc", apiVersion),
            Map(
              "query" -> """{"match_all": {}}""",
              "sort" -> """[{"price": "desc"}]"""
            ),
            settings = baseSourceSettings.withBufferSize(3).withApiVersion(apiVersion)
          )
          .map(_.source.price)
          .runWith(Sink.seq)

        val result = read.futureValue.toList
        result shouldEqual result.sorted.reverse

      }

    }

    lazy val _ = {
      //#connection-settings
      val connectionSettings = OpensearchConnectionSettings("http://localhost:9200")
        .withCredentials("user", "password")
      //#connection-settings
      //#source-settings
      val sourceSettings = OpensearchSourceSettings(connectionSettings)
        .withBufferSize(10)
        .withScrollDuration(5.minutes)
      //#source-settings
      sourceSettings.toString should startWith("OpensearchSourceSettings(")
      //#sink-settings
      val sinkSettings =
        OpensearchWriteSettings(connectionSettings)
          .withBufferSize(10)
          .withVersionType("internal")
          .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
          .withApiVersion(OpensearchApiVersion.V1)
      //#sink-settings
      sinkSettings.toString should startWith("OpensearchWriteSettings(")
      //#opensearch-params
      val OpensearchParamsV1 = OpensearchParams.V1("index")
      //#opensearch-params
      OpensearchParamsV1.toString should startWith("OpensearchParams(")
      val doc = "dummy-doc"
      //#custom-metadata-example
      val msg = WriteMessage
        .createIndexMessage(doc)
        .withCustomMetadata(Map("pipeline" -> "myPipeline"))
      //#custom-metadata-example
      msg.customMetadata should contain("pipeline")
    }

  }

}
