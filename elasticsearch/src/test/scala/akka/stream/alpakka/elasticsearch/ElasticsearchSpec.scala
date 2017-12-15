/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.http.HttpHost
import org.apache.http.entity.StringEntity
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.client.RestClient
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import spray.json._
import DefaultJsonProtocol._
import org.apache.http.message.BasicHeader

class ElasticsearchSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private val runner = new ElasticsearchClusterRunner()

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  implicit val client = RestClient.builder(new HttpHost("localhost", 9201)).build()
  //#init-client

  //#define-class
  case class Book(title: String)
  implicit val format = jsonFormat1(Book)
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
           |    "book": {
           |      "dynamic": "strict",
           |      "properties": {
           |        "title": { "type": "string"}
           |      }
           |    }
           |  }
           |}
         """.stripMargin)
    )

  private def register(indexName: String, title: String): Unit =
    client.performRequest("POST",
                          s"$indexName/book",
                          Map[String, String]().asJava,
                          new StringEntity(s"""{"title": "$title"}"""),
                          new BasicHeader("Content-Type", "application/json"))

  "Typed Elasticsearch connector" should {
    "consume and publish documents as specific type" in {
      // Copy source/book to sink2/book through typed stream
      //#run-typed
      val f1 = ElasticsearchSource
        .typed[Book](
          "source",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings(5)
        )
        .map { message: OutgoingMessage[Book] =>
          IncomingMessage(Some(message.id), message.source)
        }
        .runWith(
          ElasticsearchSink.create[Book](
            "sink2",
            "book",
            ElasticsearchSinkSettings(5)
          )
        )
      //#run-typed

      Await.result(f1, Duration.Inf)

      flush("sink2")

      // Assert docs in sink2/book
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink2",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings()
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
      // Copy source/book to sink3/book through typed stream
      //#run-flow
      val f1 = ElasticsearchSource
        .typed[Book](
          "source",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings(5)
        )
        .map { message: OutgoingMessage[Book] =>
          IncomingMessage(Some(message.id), message.source)
        }
        .via(
          ElasticsearchFlow.create[Book](
            "sink3",
            "book",
            ElasticsearchSinkSettings(5)
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink3")

      // Assert no errors
      assert(result1.forall(!_.exists(_.success == false)))

      // Assert docs in sink3/book
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink3",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings()
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
            IncomingMessage(Some(index.toString), Book(book))
        }
        .via(
          ElasticsearchFlow.create[Book](
            "sink4",
            "book",
            ElasticsearchSinkSettings(5)
          )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(f1, Duration.Inf)
      flush("sink4")

      // Assert no error
      assert(result1.forall(!_.exists(_.success == false)))

      // Assert docs in sink4/book
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink4",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings()
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
            IncomingMessage(Some(index.toString), book)
          case _ => ??? // Keep the compiler from complaining
        }
        .via(
          ElasticsearchFlow.create(
            "sink5",
            "book",
            ElasticsearchSinkSettings(maxRetry = 5, retryInterval = 100)
          )
        )
        .runWith(Sink.seq)

      val start = System.currentTimeMillis()
      val result1 = Await.result(f1, Duration.Inf)
      val end = System.currentTimeMillis()

      // Assert retired documents
      assert(
        result1.flatten.filter(!_.success).toList == Seq(
          IncomingMessageResult[JsValue](Map("subject" -> "Akka Concurrency").toJson, false)
        )
      )

      // Assert retried 5 times by looking duration
      assert(end - start > 5 * 100)

      flush("sink5")

      // Assert docs in sink5/book
      val f2 = ElasticsearchSource
        .typed[Book](
          "sink5",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings()
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
          IncomingMessage(Some(id), book, kafkaMessage.offset)
        }
        .via( // write to elastic
          ElasticsearchFlow.createWithPassThrough[Book, KafkaOffset](
            "sink6",
            "book",
            ElasticsearchSinkSettings(5)
          )
        )
        .map { messageResults =>
          messageResults.foreach { result =>
            if (!result.success) throw new Exception("Failed to write message to elastic")
            // Commit to kafka
            commitToKakfa(result.passThrough)
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
          "sink6",
          "book",
          """{"match_all": {}}""",
          ElasticsearchSourceSettings()
        )
        .map { message =>
          message.source.title
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf).toList

      result2.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
    }
  }

}
