/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import com.mongodb.reactivestreams.client.MongoClients
import org.bson.Document
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.jdk.CollectionConverters._
import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.annotation.nowarn

class MongoSourceSpec
    extends TestKit(ActorSystem("MongoSourceSpec"))
    with AnyWordSpecLike
    with MongoTest
    with ScalaFutures
    with BeforeAndAfterEach
    with Matchers
    with LogCapturing {

  java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(java.util.logging.Level.SEVERE)

  // #pojo
  case class Number(_id: Int)
  // #pojo

  // #codecs
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._

  val codecRegistry =
    fromRegistries(fromProviders(classOf[Number]: @nowarn("msg=match may not be exhaustive")), DEFAULT_CODEC_REGISTRY)
  // #codecs

  // #init-connection
  private lazy val client = MongoClients.create(ConnectionString)
  private lazy val db = client.getDatabase("MongoSourceSpec")
  private lazy val numbersColl = db
    .getCollection("numbers", classOf[Number])
    .withCodecRegistry(codecRegistry)
  // #init-connection

  private lazy val numbersDocumentColl = db.getCollection("numbers")

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = 5.seconds, interval = 50.millis)

  override def afterEach(): Unit = {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document())).runWith(Sink.head).futureValue
    super.afterEach()
  }

  private def seed() = {
    val numbers = 1 until 10
    Source
      .fromPublisher(numbersDocumentColl.insertMany {
        numbers.map { number =>
          Document.parse(s"{_id:$number}")
        }.asJava
      })
      .runWith(Sink.head)
      .futureValue
    numbers
  }

  "MongoSourceSpec" must {

    "stream the result of a simple Mongo query" in assertAllStagesStopped {
      val data: Seq[Int] = seed()

      val source: Source[Document, NotUsed] =
        MongoSource(numbersDocumentColl.find())

      val rows: Future[Seq[Document]] = source.runWith(Sink.seq)

      rows.futureValue.map(_.getInteger("_id")) must contain theSameElementsAs data
    }

    "support codec registry to read case class objects" in assertAllStagesStopped {
      val data: Seq[Number] = seed().map(Number)

      // #create-source
      val source: Source[Number, NotUsed] =
        MongoSource(numbersColl.find(classOf[Number]))
      // #create-source

      // #run-source
      val rows: Future[Seq[Number]] = source.runWith(Sink.seq)
      //#run-source

      rows.futureValue must contain theSameElementsAs data
    }

    "support multiple materializations" in assertAllStagesStopped {
      val data: Seq[Int] = seed()
      val numbersObservable = numbersDocumentColl.find()

      val source = MongoSource(numbersObservable)

      source.runWith(Sink.seq).futureValue.map(_.getInteger("_id")) must contain theSameElementsAs data
      source.runWith(Sink.seq).futureValue.map(_.getInteger("_id")) must contain theSameElementsAs data
    }

    "stream the result of Mongo query that results in no data" in assertAllStagesStopped {
      val numbersObservable = numbersDocumentColl.find()

      val rows = MongoSource(numbersObservable).runWith(Sink.seq).futureValue

      rows mustBe empty
    }
  }
}
