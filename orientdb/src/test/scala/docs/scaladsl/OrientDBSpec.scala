/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.orientdb.scaladsl._
import akka.stream.alpakka.orientdb.{
  OrientDBSourceSettings,
  OrientDBUpdateSettings,
  OrientDbReadResult,
  OrientDbWriteMessage
}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class OrientDBSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-mat

  //#init-settings
  val url = "remote:127.0.0.1:2424/"
  val dbName = "GratefulDeadConcertsScala"
  val dbUrl = s"$url$dbName"
  val username = "root"
  val password = "root"
  //#init-settings

  val source = "source2"
  val sink4 = "sink4"
  val sink5 = "sink5"
  val sink7 = "sink7"

  //#define-class
  case class Book(title: String)
  //#define-class

  //#define-client
  var oServerAdmin: OServerAdmin = _
  var oDatabase: OPartitionedDatabasePool = _
  var client: ODatabaseDocumentTx = _
  //#define-client

  override def beforeAll() = {
    //#init-db
    oServerAdmin = new OServerAdmin(url).connect(username, password)
    if (!oServerAdmin.existsDatabase(dbName, "plocal")) {
      oServerAdmin.createDatabase(dbName, "document", "plocal")
    }

    oDatabase = new OPartitionedDatabasePool(dbUrl, username, password, Runtime.getRuntime.availableProcessors(), 10)
    client = oDatabase.acquire()

    register(source)

    flush(source, "book_title", "Akka in Action")
    flush(source, "book_title", "Programming in Scala")
    flush(source, "book_title", "Learning Scala")
    flush(source, "book_title", "Scala for Spark in Production")
    flush(source, "book_title", "Scala Puzzlers")
    flush(source, "book_title", "Effective Akka")
    flush(source, "book_title", "Akka Concurrency")
  }

  override def afterAll() = {
    unregister(source)
    unregister(sink4)
    unregister(sink5)
    unregister(sink7)

    if (oServerAdmin.existsDatabase(dbName, "plocal")) {
      oServerAdmin.dropDatabase(dbName, "plocal")
    }
    oServerAdmin.close()

    client.close()
    oDatabase.close()
    TestKit.shutdownActorSystem(system)
  }

  private def register(className: String): Unit =
    if (!client.getMetadata.getSchema.existsClass(className))
      client.getMetadata.getSchema.createClass(className)

  private def flush(className: String, fieldName: String, fieldValue: String): Unit = {
    val oDocument = new ODocument()
      .field(fieldName, fieldValue)
    oDocument.setClassNameIfExists(className)
    oDocument.save()
  }

  private def unregister(className: String): Unit =
    if (client.getMetadata.getSchema.existsClass(className))
      client.getMetadata.getSchema.dropClass(className)

  "source settings" should {
    "have defaults" in {
      // #source-settings
      // re-iterating default values
      val sourceSettings = OrientDBSourceSettings(oDatabase)
        .withMaxPartitionSize(Runtime.getRuntime.availableProcessors())
        .withMaxPoolSize(-1)
        .withSkip(0)
        .withLimit(10)
      // #source-settings
      sourceSettings.toString shouldBe OrientDBSourceSettings(oDatabase).toString
    }
  }

  "write settings" should {
    "have defaults" in {
      // #write-settings
      // re-iterating default values
      val updateSettings = OrientDBUpdateSettings(oDatabase)
        .withMaxPartitionSize(Runtime.getRuntime.availableProcessors())
        .withMaxPoolSize(-1)
        .withMaxRetries(1)
        .withRetryInterval(5.seconds)
        .withBufferSize(10)
      // #write-settings
      updateSettings.toString shouldBe OrientDBUpdateSettings(oDatabase).toString
    }
  }

  "OrientDB connector" should {
    "consume and publish documents as ODocument" in {
      //Copy source to sink1 through ODocument stream
      val f1 = OrientDBSource(
        source,
        OrientDBSourceSettings(oDatabasePool = oDatabase)
      ).map { message: OrientDbReadResult[ODocument] =>
          OrientDbWriteMessage(message.oDocument)
        }
        .runWith(
          OrientDBSink(
            sink4,
            OrientDBUpdateSettings(oDatabasePool = oDatabase)
          )
        )

      Await.ready(f1, Duration.Inf)

      //#run-odocument
      val f2 = OrientDBSource(
        sink4,
        OrientDBSourceSettings(oDatabasePool = oDatabase)
      ).map { message =>
          message.oDocument.field[String]("book_title")
        }
        .runWith(Sink.seq)
      //#run-odocument

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

  "OrientDBFlow" should {
    "store ODocuments and pass ODocuments" in {
      // Copy source/book to sink3/book through ODocument stream
      //#run-flow

      val f1 = OrientDBSource(
        source,
        OrientDBSourceSettings(oDatabasePool = oDatabase)
      ).map { message: OrientDbReadResult[ODocument] =>
          OrientDbWriteMessage(message.oDocument)
        }
        .via(
          OrientDBFlow.create(
            sink5,
            OrientDBUpdateSettings(oDatabasePool = oDatabase)
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      Await.ready(f1, Duration.Inf)

      val f2 = OrientDBSource(
        sink5,
        OrientDBSourceSettings(oDatabasePool = oDatabase)
      ).map { message =>
          message.oDocument.field[String]("book_title")
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

  "OrientDBFlow" should {
    "kafka-example - store documents and pass Responses with passThrough" in {
      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to oRIENTdb, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book 1"), KafkaOffset(0)),
        KafkaMessage(Book("Book 2"), KafkaOffset(1)),
        KafkaMessage(Book("Book 3"), KafkaOffset(2))
      )

      var committedOffsets = List[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val f1 = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title
          println("title: " + book.title)

          OrientDbWriteMessage(new ODocument().field("book_title", id), kafkaMessage.offset)
        }
        .via(
          OrientDBFlow.createWithPassThrough(
            sink7,
            OrientDBUpdateSettings(oDatabase)
          )
        )
        .map { messages: Seq[OrientDbWriteMessage[ODocument, KafkaOffset]] =>
          messages.foreach { message =>
            commitToKafka(message.passThrough)
          }
        }
        .runWith(Sink.seq)

      Await.ready(f1, Duration.Inf)
      //#kafka-example
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      val f2 = OrientDBSource(
        sink7,
        OrientDBSourceSettings(oDatabasePool = oDatabase)
      ).map { message =>
          message.oDocument.field[String]("book_title")
        }
        .runWith(Sink.seq)

      val result2 = Await.result(f2, Duration.Inf)

      result2.sorted shouldEqual Seq(
        "Book 1",
        "Book 2",
        "Book 3"
      )
    }
  }
}
