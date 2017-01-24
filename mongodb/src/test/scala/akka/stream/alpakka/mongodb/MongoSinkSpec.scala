/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mongodb

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.mongodb.scaladsl.MongoSink
import akka.stream.scaladsl.Source
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._

class MongoSinkSpec
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  override protected def beforeAll(): Unit =
    Await.result(db.drop().toFuture(), 5.seconds)

  private val client = MongoClient(s"mongodb://localhost:27017")
  private val db = client.getDatabase("alpakka-mongo")
  private val numbersColl = db.getCollection("numbersSink")

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 50.millis)

  override def afterEach(): Unit =
    Await.result(numbersColl.deleteMany(Document()).toFuture(), 5.seconds)

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)

  "MongoSinkSpec" must {

    "stream the data into Mongo" in {
      val testRange = 0 until 10

      val source = Source(testRange).map(i => Document(s"""{"value":$i}"""))

      val result = source.runWith(MongoSink(2, numbersColl))

      result.futureValue

      val found = numbersColl.find().toFuture().futureValue

      found.map(_.getInteger("value")) must contain theSameElementsAs testRange
    }
  }

  private class ParadoxSnippet() {
    val source: Source[Document, NotUsed] = ???
    //#create-sink
    source.runWith(MongoSink(parallelism = 2, collection = numbersColl))
    //#create-sink
  }
}
