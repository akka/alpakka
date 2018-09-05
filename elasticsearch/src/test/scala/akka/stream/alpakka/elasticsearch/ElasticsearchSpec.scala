/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.http.entity.StringEntity
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import org.apache.http.message.BasicHeader

class ElasticsearchSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private val runner = new ElasticsearchClusterRunner()

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.elasticsearch.client.RestClient
  import org.apache.http.HttpHost

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
      s"$indexName",
      Map[String, String]().asJava,
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

  private def register(indexName: String, title: String): Unit =
    client.performRequest("POST",
                          s"$indexName/_doc",
                          Map[String, String]().asJava,
                          new StringEntity(s"""{"title": "$title"}"""),
                          new BasicHeader("Content-Type", "application/json"))

  private def documentation: Unit = {
    //#source-settings
    val sourceSettings = ElasticsearchSourceSettings(bufferSize = 10)
    //#source-settings
    //#sink-settings
    val sinkSettings =
      ElasticsearchSinkSettings(bufferSize = 10, retryInterval = 5000, maxRetry = 100, retryPartialFailure = true)
    //#sink-settings
  }

  "Un-typed Elasticsearch connector" should {
    "consume and publish Json documents" in {
      // Copy source/_doc to sink2/_doc through typed stream
      //#run-jsobject
      val f1 = ElasticsearchSource
        .create(
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: OutgoingMessage[spray.json.JsObject] =>
          val book: Book = jsonReader[Book].read(message.source)
          IncomingIndexMessage(message.id, book)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            indexName = "sink2",
            typeName = "_doc"
          )
        )
      //#run-jsobject

      Await.result(f1, Duration.Inf)

      flush("sink2")

      // Assert docs in sink2/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink2",
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result = Await.result(f2, Duration.Inf)

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
  }

  "Typed Elasticsearch connector" should {
    "consume and publish documents as specific type" in {
      // Copy source/_doc to sink2/_doc through typed stream
      //#run-typed
      val f1 = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: OutgoingMessage[Book] =>
          IncomingIndexMessage(message.id, message.source)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            indexName = "sink2",
            typeName = "_doc"
          )
        )
      //#run-typed

      Await.result(f1, Duration.Inf)

      flush("sink2")

      // Assert docs in sink2/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink2",
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result = Await.result(f2, Duration.Inf)

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
  }

  "ElasticsearchFlow" should {
    "store documents and pass failed documents to downstream" in {
      // Copy source/_doc to sink3/_doc through typed stream
      //#run-flow
      val f1 = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: OutgoingMessage[Book] =>
          IncomingIndexMessage(message.id, message.source)
        }
        .via(
          ElasticsearchFlow.create[Book](
            indexName = "sink3",
            typeName = "_doc"
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink3")

      // Assert no errors
      assert(result1.forall(!_.exists(_.success == false)))

      // Assert docs in sink3/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink3",
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf)

      result2.sorted shouldEqual Seq(
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
    "not post invalid encoded JSON" in {

      val books = Seq(
        "Akka in Action",
        "Akka \u00DF Concurrency"
      )

      val f1 = Source(books.zipWithIndex.toVector)
        .map {
          case (book: String, index: Int) =>
            IncomingIndexMessage(index.toString, Book(book))
        }
        .via(
          ElasticsearchFlow.create[Book](
            "sink4",
            "_doc"
          )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink4")

      // Assert no error
      assert(result1.forall(!_.exists(_.success == false)))

      // Assert docs in sink4/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink4",
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf)

      result2.sorted shouldEqual Seq(
        "Akka in Action",
        "Akka \u00DF Concurrency"
      )
    }
  }

  "ElasticsearchFlow" should {
    "retry a failed documents and pass retired documents to downstream" in {
      // Create strict mapping to prevent invalid documents
      createStrictMapping("sink5")

      val f1 = Source(
        Seq(
          Map("title" -> "Akka in Action").toJson,
          Map("subject" -> "Akka Concurrency").toJson
        ).zipWithIndex.toVector
      ).map {
          case (book: JsObject, index: Int) =>
            IncomingIndexMessage(index.toString, book)
          case _ => ??? // Keep the compiler from complaining
        }
        .via(
          ElasticsearchFlow.create(
            "sink5",
            "_doc",
            ElasticsearchSinkSettings(maxRetry = 5, retryInterval = 100, retryPartialFailure = true)
          )
        )
        .runWith(Sink.seq)

      val start = System.currentTimeMillis()
      val result1 = Await.result(f1, Duration.Inf)
      val end = System.currentTimeMillis()

      // Assert retired documents
      assert(
        result1.flatten.filter(!_.success).toList == Seq(
          IncomingMessageResult[JsValue, NotUsed](
            IncomingIndexMessage("1", Map("subject" -> "Akka Concurrency").toJson),
            Some(
              """{"type":"strict_dynamic_mapping_exception","reason":"mapping set to strict, dynamic introduction of [subject] within [_doc] is not allowed"}"""
            )
          )
        )
      )

      // Assert retried 5 times by looking duration
      assert(end - start > 5 * 100)

      flush("sink5")

      // Assert docs in sink5/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink5",
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf)

      result2.sorted shouldEqual Seq(
        "Akka in Action"
      )
    }
  }

  "ElasticsearchFlow" should {
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

      var committedOffsets = List[KafkaOffset]()

      def commitToKakfa(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val f1 = Source(messagesFromKafka) // Assume we get this from Kafka
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title
          println("title: " + book.title)

          // Transform message so that we can write to elastic
          IncomingIndexMessage(id, book).withPassThrough(kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            indexName = "sink6",
            typeName = "_doc"
          )
        )
        .map { messageResults =>
          messageResults.foreach { result =>
            if (!result.success) throw new Exception("Failed to write message to elastic")
            // Commit to kafka
            commitToKakfa(result.message.passThrough)
          }
        }
        .runWith(Sink.seq)

      Await.ready(f1, Duration.Inf)
      //#kafka-example
      flush("sink6")

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      // Assert that all docs were written to elastic
      val f2 = ElasticsearchSource
        .typed[Book](
          indexName = "sink6",
          typeName = "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf).toList

      result2.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
    }
  }

  "ElasticsearchFlow" should {
    "store new documents using upsert method and partially update existing ones" in {
      val books = List(
        ("00001", Book("Book 1")),
        ("00002", Book("Book 2")),
        ("00003", Book("Book 3"))
      )

      // Create new documents in sink7/_doc using the upsert method
      val f1 = Source(books)
        .map { book: (String, Book) =>
          IncomingUpsertMessage(id = book._1, source = book._2)
        }
        .via(
          ElasticsearchFlow.create[Book](
            "sink7",
            "_doc"
          )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink7")

      // Assert no errors
      assert(result1.forall(!_.exists(_.success == false)))

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
      val f2 = Source(updatedBooks)
        .map { book: (String, JsObject) =>
          IncomingUpsertMessage(id = book._1, source = book._2)
        }
        .via(
          ElasticsearchFlow.create[JsObject](
            "sink7",
            "_doc"
          )
        )
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf)
      flush("sink7")

      // Assert no errors
      assert(result2.forall(!_.exists(_.success == false)))

      // Assert docs in sink7/_doc
      val f3 = ElasticsearchSource(
        "sink7",
        "_doc",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings.Default
      ).map { message =>
          message.source
        }
        .runWith(Sink.seq)

      val result3 = Await.result(f3, Duration.Inf)

      // Docs should contain both columns
      result3.sortBy(_.fields("title").compactPrint) shouldEqual Seq(
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
  }

  "ElasticsearchFlow" should {
    "handle multiple types of operations correctly" in {
      //#multiple-operations
      // Create, update, upsert and delete documents in sink8/_doc
      val requests = List[IncomingMessage[Book, NotUsed]](
        IncomingIndexMessage(id = "00001", source = Book("Book 1")),
        IncomingUpsertMessage(id = "00002", source = Book("Book 2")),
        IncomingUpsertMessage(id = "00003", source = Book("Book 3")),
        IncomingUpdateMessage(id = "00004", source = Book("Book 4")),
        IncomingDeleteMessage(id = "00002")
      )

      val f1 = Source(requests)
        .via(
          ElasticsearchFlow.create[Book](
            "sink8",
            "_doc"
          )
        )
        .runWith(Sink.seq)
      //#multiple-operations

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink8")

      // Assert no errors except a missing document for a update request
      val error = result1.flatMap(_.flatMap(_.error))
      error.length shouldEqual 1
      error(0).parseJson.asJsObject.fields("reason").asInstanceOf[JsString].value shouldEqual
      "[_doc][00004]: document missing"

      // Assert docs in sink8/_doc
      val f3 = ElasticsearchSource(
        "sink8",
        "_doc",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings.Default
      ).map { message =>
          message.source
        }
        .runWith(Sink.seq)

      val result3 = Await.result(f3, Duration.Inf)

      // Docs should contain both columns
      result3.sortBy(_.fields("title").compactPrint) shouldEqual Seq(
        JsObject("title" -> JsString("Book 1")),
        JsObject("title" -> JsString("Book 3"))
      )
    }
  }

  "ElasticsearchSource" should {
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
      val f1 = Source(docs)
        .map { doc =>
          IncomingIndexMessage(doc.id, doc)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchSinkSettings(bufferSize = 5)
          )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(f1, Duration.Inf)
      flush(indexName)

      // Assert no errors
      assert(result1.forall(!_.exists(_.success == false)))

      // search for the documents and assert them being at version 1,
      // then update while specifying that for which version

      val f3 = ElasticsearchSource
        .typed[VersionTestDoc](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings(includeDocumentVersion = true)
        )
        .map { message =>
          val doc = message.source
          val version = message.version.get
          assert(1 == version) // Assert document got version = 1

          // Update it

          val newDoc = doc.copy(value = doc.value + 1)

          IncomingIndexMessage(newDoc.id, newDoc).withVersion(version)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchSinkSettings(bufferSize = 5)
          )
        )
        .runWith(Sink.seq)

      val result3 = Await.result(f3, Duration.Inf)
      assert(result3.forall(!_.exists(_.success == false)))

      flush(indexName)
      // Search again to assert that all documents are now on version 2
      val f4 = ElasticsearchSource
        .typed[VersionTestDoc](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings(includeDocumentVersion = true)
        )
        .map { message =>
          val doc = message.source
          val version = message.version.get
          assert(doc.value == 1)
          assert(2 == version) // Assert document got version = 2
          doc
        }
        .runWith(Sink.ignore)

      val result4 = Await.result(f4, Duration.Inf)

      // Try to write document with old version - it should fail

      val f5 = Source
        .single(VersionTestDoc("1", "a", 2))
        .map { doc =>
          val oldVersion = 1
          IncomingIndexMessage(doc.id, doc).withVersion(oldVersion)
        }
        .via(
          ElasticsearchFlow.create[VersionTestDoc](
            indexName,
            typeName,
            ElasticsearchSinkSettings(bufferSize = 5)
          )
        )
        .runWith(Sink.seq)

      val result5 = Await.result(f5, Duration.Inf)
      assert(result5(0)(0).success == false)

    }

    "allow read and write using configured version type" in {

      val indexName = "book-test-version-type"
      val typeName = "_doc"

      val book = Book("A sample title")
      val docId = "1"
      val externalVersion = 5L

      // Insert new document using external version
      val f1 = Source
        .single(book)
        .map { doc =>
          IncomingIndexMessage(docId, doc).withVersion(externalVersion)
        }
        .via(
          ElasticsearchFlow.create[Book](
            indexName,
            typeName,
            ElasticsearchSinkSettings(bufferSize = 5, versionType = Some("external"))
          )
        )
        .runWith(Sink.seq)

      val insertResult = Await.result(f1, Duration.Inf).head.head
      assert(insertResult.success)

      flush(indexName)

      // Assert that the document's external version is saved
      val f2 = ElasticsearchSource
        .typed[Book](
          indexName,
          typeName,
          """{"match_all": {}}""",
          ElasticsearchSourceSettings(includeDocumentVersion = true)
        )
        .runWith(Sink.head)

      val message = Await.result(f2, Duration.Inf)
      assert(message.version.contains(externalVersion))
    }
  }

  "Elasticsearch connector" should {
    "Should use indexName supplied in message if present" in {
      // Copy source/_doc to sink2/_doc through typed stream

      //#custom-index-name-example
      val customIndexName = "custom-index"

      val f1 = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = "_doc",
          query = """{"match_all": {}}"""
        )
        .map { message: OutgoingMessage[Book] =>
          IncomingIndexMessage(message.id, message.source)
            .withIndexName(customIndexName) // Setting the index-name to use for this document
        }
        .runWith(
          ElasticsearchSink.create[Book](
            indexName = "this-is-not-the-index-we-are-using",
            typeName = "_doc"
          )
        )
      //#custom-index-name-example

      Await.result(f1, Duration.Inf)

      flush(customIndexName)

      // Assert docs in custom-index/_doc
      val f2 = ElasticsearchSource
        .typed[Book](
          customIndexName,
          "_doc",
          """{"match_all": {}}"""
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result = Await.result(f2, Duration.Inf)

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
  }

  "ElasticsearchSource" should {
    "should let you search without specifying typeName" in {
      val f1 = ElasticsearchSource
        .typed[Book](
          indexName = "source",
          typeName = None,
          query = """{"match_all": {}}""",
          settings = ElasticsearchSourceSettings(bufferSize = 5)
        )
        .map(_.source.title)
        .runWith(Sink.seq)

      val result = Await.result(f1, Duration.Inf).toList

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
  }

  "ElasticsearchSource" should {
    "be able to use custom searchParams" in {

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
      val f1 = Source(docs)
        .map { doc =>
          IncomingIndexMessage(doc.id, doc)
        }
        .via(
          ElasticsearchFlow.create[TestDoc](
            indexName,
            typeName,
            ElasticsearchSinkSettings(bufferSize = 5)
          )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(f1, Duration.Inf)
      flush(indexName)

      // Assert no errors
      assert(result1.forall(!_.exists(_.success == false)))

      //#custom-search-params
      // Search for docs and ask elastic to only return some fields

      val f3 = ElasticsearchSource
        .typed[TestDoc](indexName,
                        Some(typeName),
                        searchParams = Map(
                          "query" -> """ {"match_all": {}} """,
                          "_source" -> """ ["id", "a", "c"] """
                        ),
                        ElasticsearchSourceSettings.Default)
        .map { message =>
          message.source
        }
        .runWith(Sink.seq)

      //#custom-search-params

      val result3 = Await.result(f3, Duration.Inf)
      assert(result3.toList.sortBy(_.id) == docs.map(_.copy(b = None)))

    }
  }

  def compileOnlySample(): Unit = {
    val doc = "dummy-doc"
    //#custom-metadata-example
    val msg = IncomingIndexMessage(doc)
      .withCustomMetadata(Map("pipeline" -> "myPipeline"))
    //#custom-metadata-example
  }

}
