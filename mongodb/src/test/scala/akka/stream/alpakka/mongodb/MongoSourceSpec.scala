package akka.stream.alpakka.mongodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.Sink
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

class MongoSourceSpec
  extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  implicit val system = ActorSystem()

  override protected def beforeAll(): Unit = {
    Await.result(db.drop().toFuture(), 5.seconds)
  }

  implicit val mat = ActorMaterializer()

  val db = Mongo.db
  val numbersColl = db.getCollection("numbers")

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 50.millis)

  override def afterEach(): Unit = {
    Await.result(numbersColl.deleteMany(Document()).toFuture(), 5.seconds)
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  def seed() = {
    val numbers = 1 until 10
    Await.result(
      numbersColl.insertMany {
        numbers.map { number => Document(s"{_id:$number}") }
      }.toFuture, 5.seconds)
    numbers
  }

  "MongoSourceSpec" must {

    "stream the result of a simple Mongo query" in {
      val data: Seq[Int] = seed()
      val numbersObservable = numbersColl.find()

      val rows = MongoSource(numbersObservable).runWith(Sink.seq).futureValue

      rows.map(_ \ "_id").map(_.as[Int]) must contain theSameElementsAs data
    }

    "support multiple materializations" in {
      val data: Seq[Int] = seed()
      val numbersObservable = numbersColl.find()

      val rows = MongoSource(numbersObservable).runWith(Sink.seq).futureValue

      rows.map(_ \ "_id").map(_.as[Int]) must contain theSameElementsAs data
      rows.map(_ \ "_id").map(_.as[Int]) must contain theSameElementsAs data
    }

    "stream the result of Mongo query that results in no data" in {
      val numbersObservable = numbersColl.find()

      val rows = MongoSource(numbersObservable).runWith(Sink.seq).futureValue

      rows mustBe empty
    }

  }
}
