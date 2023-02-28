/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.mongodb.{DocumentReplace, DocumentUpdate}
import akka.stream.alpakka.mongodb.scaladsl.MongoSink
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.mongodb.client.model.{Filters, InsertManyOptions, Updates}
import com.mongodb.reactivestreams.client.{MongoClients, MongoCollection}
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MongoSinkSpec
    extends AnyWordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {

  // case class and codec for mongodb macros
  case class Number(_id: Int)
  case class DomainObject(_id: Int, firstProperty: String, secondProperty: String)

  val codecRegistry =
    fromRegistries(fromProviders(classOf[Number], classOf[DomainObject]), DEFAULT_CODEC_REGISTRY)

  implicit val system = ActorSystem()

  override protected def beforeAll(): Unit =
    Source.fromPublisher(db.drop()).runWith(Sink.headOption).futureValue

  private val client = MongoClients.create(s"mongodb://localhost:27017")
  private val db = client.getDatabase("MongoSinkSpec").withCodecRegistry(codecRegistry)
  private val numbersColl: MongoCollection[Number] =
    db.getCollection("numbersSink", classOf[Number]).withCodecRegistry(codecRegistry)
  private val numbersDocumentColl = db.getCollection("numbersSink")
  private val domainObjectsColl: MongoCollection[DomainObject] =
    db.getCollection("domainObjectsSink", classOf[DomainObject]).withCodecRegistry(codecRegistry)
  private val domainObjectsDocumentColl = db.getCollection("domainObjectsSink")

  implicit val defaultPatience =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  override def afterEach(): Unit = {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document())).runWith(Sink.head).futureValue
    Source.fromPublisher(domainObjectsDocumentColl.deleteMany(new Document())).runWith(Sink.head).futureValue
  }

  override def afterAll(): Unit =
    system.terminate().futureValue

  val testRange = 0 until 10

  def insertTestRange(): Unit =
    Source
      .fromPublisher(numbersDocumentColl.insertMany(testRange.map(i => Document.parse(s"""{"value":$i}""")).asJava))
      .runWith(Sink.head)
      .futureValue

  def insertDomainObjectsRange(): Unit =
    Source
      .fromPublisher(
        domainObjectsColl.insertMany(
          testRange.map(i => DomainObject(i, s"first-property-$i", s"second-property-$i")).asJava
        )
      )
      .runWith(Sink.head)
      .futureValue

  "MongoSinkSpec" must {

    "save with insertOne" in assertAllStagesStopped {
      val source = Source(testRange).map(i => Document.parse(s"""{"value":$i}"""))
      val completion = source.runWith(MongoSink.insertOne(numbersDocumentColl))

      completion.futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found.map(_.getInteger("value")) must contain theSameElementsAs testRange
    }

    "save with insertOne and codec support" in assertAllStagesStopped {
      // #insert-one
      val testRangeObjects = testRange.map(Number)
      val source = Source(testRangeObjects)
      source.runWith(MongoSink.insertOne(numbersColl)).futureValue
      // #insert-one

      val found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq).futureValue

      found must contain theSameElementsAs testRangeObjects
    }

    "save with insertMany" in assertAllStagesStopped {
      val source = Source(testRange).map(i => Document.parse(s"""{"value":$i}"""))

      source.grouped(2).runWith(MongoSink.insertMany(numbersDocumentColl)).futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found.map(_.getInteger("value")) must contain theSameElementsAs testRange
    }

    "save with insertMany and codec support" in assertAllStagesStopped {
      // #insert-many
      val objects = testRange.map(Number)
      val source = Source(objects)
      val completion = source.grouped(2).runWith(MongoSink.insertMany[Number](numbersColl))
      // #insert-many

      completion.futureValue

      val found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq).futureValue

      found must contain theSameElementsAs objects
    }

    "save with insertMany with options" in assertAllStagesStopped {
      val source = Source(testRange).map(i => Document.parse(s"""{"value":$i}"""))

      source
        .grouped(2)
        .runWith(MongoSink.insertMany(numbersDocumentColl, new InsertManyOptions().ordered(false)))
        .futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found.map(_.getInteger("value")) must contain theSameElementsAs testRange
    }

    "save with insertMany with options and codec support" in assertAllStagesStopped {
      val testRangeObjects = testRange.map(Number)
      val source = Source(testRangeObjects)

      source
        .grouped(2)
        .runWith(MongoSink.insertMany[Number](numbersColl, new InsertManyOptions().ordered(false)))
        .futureValue

      val found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq).futureValue

      found must contain theSameElementsAs testRangeObjects
    }

    "update with updateOne" in assertAllStagesStopped {
      insertTestRange()

      // #update-one
      val source = Source(testRange).map(
        i => DocumentUpdate(filter = Filters.eq("value", i), update = Updates.set("updateValue", i * -1))
      )
      val completion = source.runWith(MongoSink.updateOne(numbersDocumentColl))
      // #update-one

      completion.futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found.map(doc => doc.getInteger("value") -> doc.getInteger("updateValue")) must contain theSameElementsAs testRange
        .map(i => i -> i * -1)
    }

    "update with updateMany" in assertAllStagesStopped {
      insertTestRange()

      val source = Source
        .single(0)
        .map(
          _ => DocumentUpdate(filter = Filters.gte("value", 0), update = Updates.set("updateValue", 0))
        )

      source.runWith(MongoSink.updateMany(numbersDocumentColl)).futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found.map(doc => doc.getInteger("value") -> doc.getInteger("updateValue")) must contain theSameElementsAs testRange
        .map(i => i -> 0)
    }

    "delete with deleteOne" in assertAllStagesStopped {
      insertTestRange()

      // #delete-one
      val source = Source(testRange).map(i => Filters.eq("value", i))
      val completion = source.runWith(MongoSink.deleteOne(numbersDocumentColl))
      // #delete-one

      completion.futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found mustBe empty
    }

    "delete with deleteMany" in assertAllStagesStopped {
      insertTestRange()

      val source = Source.single(0).map(_ => Filters.gte("value", 0))

      source.runWith(MongoSink.deleteMany(numbersDocumentColl)).futureValue

      val found = Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq).futureValue

      found mustBe empty
    }

    "replace with replaceOne and codec support" in assertAllStagesStopped {
      insertDomainObjectsRange()
      val updatedObjects =
        testRange.map(i => DomainObject(i, s"updated-first-property-$i", s"updated-second-property-$i"))

      // #replace-one
      val source = Source(testRange).map(
        i =>
          DocumentReplace[DomainObject](
            filter = Filters.eq("_id", i),
            replacement = DomainObject(i, s"updated-first-property-$i", s"updated-second-property-$i")
          )
      )
      val completion = source.runWith(MongoSink.replaceOne[DomainObject](domainObjectsColl))
      // #replace-one

      completion.futureValue

      val found = Source.fromPublisher(domainObjectsColl.find()).runWith(Sink.seq).futureValue

      found must contain theSameElementsAs updatedObjects
    }
  }
}
