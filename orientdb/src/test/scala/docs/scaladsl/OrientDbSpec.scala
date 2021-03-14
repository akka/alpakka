/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.orientdb.scaladsl._
import akka.stream.alpakka.orientdb.{
  OrientDbReadResult,
  OrientDbSourceSettings,
  OrientDbWriteMessage,
  OrientDbWriteSettings
}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
//#init-settings
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
//#init-settings
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.record.impl.ODocument
import docs.javadsl.OrientDbTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OrientDbSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with LogCapturing {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  implicit val system: ActorSystem = ActorSystem()

  //#init-settings

  val url = "remote:127.0.0.1:2424/"
  val dbName = "GratefulDeadConcertsScala"
  val dbUrl = s"$url$dbName"
  val username = "root"
  val password = "root"
  //#init-settings

  val sourceClass = "source1"
  val sinkClass2 = "sink2"
  val sink4 = "sink4"
  val sink5 = "sink5"
  val sink7 = "sink7"

  //#define-class
  case class Book(title: String)
  //#define-class

  var oServerAdmin: OServerAdmin = _
  var oDatabase: OPartitionedDatabasePool = _
  var client: ODatabaseDocumentTx = _

  override def beforeAll() = {
    oServerAdmin = new OServerAdmin(url).connect(username, password)
    if (!oServerAdmin.existsDatabase(dbName, "plocal")) {
      oServerAdmin.createDatabase(dbName, "document", "plocal")
    }

    //#init-settings

    val oDatabase: OPartitionedDatabasePool =
      new OPartitionedDatabasePool(dbUrl, username, password, Runtime.getRuntime.availableProcessors(), 10)

    system.registerOnTermination(() -> oDatabase.close())
    //#init-settings
    this.oDatabase = oDatabase
    client = oDatabase.acquire()

    register(sourceClass)

    flush(sourceClass, "book_title", "Akka in Action")
    flush(sourceClass, "book_title", "Programming in Scala")
    flush(sourceClass, "book_title", "Learning Scala")
    flush(sourceClass, "book_title", "Scala for Spark in Production")
    flush(sourceClass, "book_title", "Scala Puzzlers")
    flush(sourceClass, "book_title", "Effective Akka")
    flush(sourceClass, "book_title", "Akka Concurrency")
  }

  override def afterAll() = {
    unregister(sourceClass)
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
    "have defaults" in assertAllStagesStopped {
      // #source-settings
      // re-iterating default values
      val sourceSettings = OrientDbSourceSettings(oDatabase)
        .withSkip(0)
        .withLimit(10)
      // #source-settings
      sourceSettings.toString shouldBe OrientDbSourceSettings(oDatabase).toString
    }
  }

  "write settings" should {
    "have defaults" in assertAllStagesStopped {
      // #write-settings
      // re-iterating default values
      val updateSettings = OrientDbWriteSettings(oDatabase)
      // #write-settings
      updateSettings.toString shouldBe OrientDbWriteSettings(oDatabase).toString
    }
  }

  "OrientDB connector" should {
    "consume and publish documents as ODocument" in assertAllStagesStopped {
      //Copy source to sink1 through ODocument stream
      val f1 = OrientDbSource(
        sourceClass,
        OrientDbSourceSettings(oDatabase)
      ).map { message: OrientDbReadResult[ODocument] =>
          OrientDbWriteMessage(message.oDocument)
        }
        .groupedWithin(10, 50.millis)
        .runWith(
          OrientDbSink(
            sink4,
            OrientDbWriteSettings(oDatabase)
          )
        )

      f1.futureValue shouldBe Done

      //#run-odocument
      val result: Future[immutable.Seq[String]] = OrientDbSource(
        sink4,
        OrientDbSourceSettings(oDatabase)
      ).map { message: OrientDbReadResult[ODocument] =>
          message.oDocument.field[String]("book_title")
        }
        .runWith(Sink.seq)
      //#run-odocument

      result.futureValue.sorted shouldEqual Seq(
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
    "store ODocuments and pass ODocuments" in assertAllStagesStopped {
      // Copy source/book to sink3/book through ODocument stream
      //#run-flow

      val f1 = OrientDbSource(
        sourceClass,
        OrientDbSourceSettings(oDatabase)
      ).map { message: OrientDbReadResult[ODocument] =>
          OrientDbWriteMessage(message.oDocument)
        }
        .groupedWithin(10, 50.millis)
        .via(
          OrientDbFlow.create(
            sink5,
            OrientDbWriteSettings(oDatabase)
          )
        )
        .runWith(Sink.seq)
      //#run-flow

      f1.futureValue.flatten should have('size (7))

      val f2 = OrientDbSource(
        sink5,
        OrientDbSourceSettings(oDatabase)
      ).map { message =>
          message.oDocument.field[String]("book_title")
        }
        .runWith(Sink.seq)

      f2.futureValue.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "support typed source and sink" in assertAllStagesStopped {
      // #run-typed
      val streamCompletion: Future[Done] = OrientDbSource
        .typed(sourceClass, OrientDbSourceSettings(oDatabase), classOf[OrientDbTest.source1])
        .map { m: OrientDbReadResult[OrientDbTest.source1] =>
          val db: ODatabaseDocumentTx = oDatabase.acquire
          db.setDatabaseOwner(new OObjectDatabaseTx(db))
          ODatabaseRecordThreadLocal.instance.set(db)
          val sink: OrientDbTest.sink2 = new OrientDbTest.sink2
          sink.setBook_title(m.oDocument.getBook_title)
          OrientDbWriteMessage(sink)
        }
        .groupedWithin(10, 10.millis)
        .runWith(OrientDbSink.typed(sinkClass2, OrientDbWriteSettings.create(oDatabase), classOf[OrientDbTest.sink2]))
      // #run-typed

      streamCompletion.futureValue shouldBe Done

    }

    "kafka-example - store documents and pass Responses with passThrough" in assertAllStagesStopped {
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
        .groupedWithin(10, 50.millis)
        .via(
          OrientDbFlow.createWithPassThrough(
            sink7,
            OrientDbWriteSettings(oDatabase)
          )
        )
        .map { messages: Seq[OrientDbWriteMessage[ODocument, KafkaOffset]] =>
          messages.foreach { message =>
            commitToKafka(message.passThrough)
          }
        }
        .runWith(Sink.ignore)

      //#kafka-example
      f1.futureValue shouldBe Done
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      val f2 = OrientDbSource(
        sink7,
        OrientDbSourceSettings(oDatabase)
      ).map { message =>
          message.oDocument.field[String]("book_title")
        }
        .runWith(Sink.seq)

      f2.futureValue.sorted shouldEqual Seq(
        "Book 1",
        "Book 2",
        "Book 3"
      )
    }
  }
}
