/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import slick.basic.DatabaseConfig
import slick.dbio.DBIOAction
import slick.jdbc.{GetResult, JdbcProfile}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * This unit test is run using a local H2 database using
 * `/tmp/alpakka-slick-h2-test` for temporary storage.
 */
class SlickSpec
    extends AnyWordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {
  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-session
  implicit val session: SlickSession = SlickSession.forConfig("slick-h2")
  //#init-session

  import session.profile.api._

  case class User(id: Int, name: String)
  class Users(tag: Tag) extends Table[(Int, String)](tag, "ALPAKKA_SLICK_SCALADSL_TEST_USERS") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def * = (id, name)
  }

  implicit val ec = system.dispatcher
  implicit val defaultPatience = PatienceConfig(timeout = 3.seconds, interval = 50.millis)
  implicit val getUserResult = GetResult(r => User(r.nextInt, r.nextString))

  val users = (1 to 40).map(i => User(i, s"Name$i")).toSet

  val createTable = sqlu"""CREATE TABLE ALPAKKA_SLICK_SCALADSL_TEST_USERS(ID INTEGER, NAME VARCHAR(50))"""
  val dropTable = sqlu"""DROP TABLE ALPAKKA_SLICK_SCALADSL_TEST_USERS"""
  val selectAllUsers = sql"SELECT ID, NAME FROM ALPAKKA_SLICK_SCALADSL_TEST_USERS".as[User]
  val typedSelectAllUsers = TableQuery[Users].result

  def insertUser(user: User): DBIO[Int] =
    sqlu"INSERT INTO ALPAKKA_SLICK_SCALADSL_TEST_USERS VALUES(${user.id}, ${user.name})"

  def getAllUsersFromDb: Future[Set[User]] = Slick.source(selectAllUsers).runWith(Sink.seq).map(_.toSet)
  def populate() = {
    val actions = users.map(insertUser)

    // This uses the standard Slick API exposed by the Slick session
    // on purpose, just to double-check that inserting data through
    // our Alpakka connectors is equivalent to inserting it the Slick way.
    session.db.run(DBIO.seq(actions.toList: _*)).futureValue
  }

  override def beforeEach(): Unit = session.db.run(createTable).futureValue
  override def afterEach(): Unit = session.db.run(dropTable).futureValue

  override def afterAll(): Unit = {
    //#close-session
    system.registerOnTermination(() => session.close())
    //#close-session

    TestKit.shutdownActorSystem(system)
  }

  "SlickSession.forDbAndProfile" must {
    "create a slick session able to talk to the db" in {
      //#init-session-from-db-and-profile
      val db = Database.forConfig("slick-h2.db")
      val profile = slick.jdbc.H2Profile
      val slickSessionCreatedForDbAndProfile: SlickSession = SlickSession.forDbAndProfile(db, profile)
      //#init-session-from-db-and-profile
      try {
        val q = sql"select true".as[Boolean]
        val result = Slick
          .source(q)(slickSessionCreatedForDbAndProfile)
          .runWith(Sink.head)
          .futureValue
        assert(result === true)
      } finally {
        slickSessionCreatedForDbAndProfile.close()
      }
    }
  }

  "Slick.source(...)" must {
    def tupleToUser(columns: (Int, String)): User = User(columns._1, columns._2)

    "stream the result of a Slick plain SQL query" in {
      populate()

      getAllUsersFromDb.futureValue mustBe users
    }

    "stream the result of a Slick plain SQL query that results in no data" in {
      getAllUsersFromDb.futureValue mustBe empty
    }

    "stream the result of a Slick typed query" in {
      populate()

      val foundUsers =
        Slick
          .source(typedSelectAllUsers)
          .map(tupleToUser)
          .runWith(Sink.seq)
          .futureValue

      foundUsers must contain theSameElementsAs users
    }

    "stream the result of a Slick typed query that results in no data" in {
      val foundUsers =
        Slick
          .source(typedSelectAllUsers)
          .runWith(Sink.seq)
          .futureValue

      foundUsers mustBe empty
    }

    "support multiple materializations" in {
      populate()

      val source = Slick.source(selectAllUsers)

      source.runWith(Sink.seq).futureValue.toSet mustBe users
      source.runWith(Sink.seq).futureValue.toSet mustBe users
    }
  }

  "Slick.flow(..)" must {
    "insert 40 records into a table (no parallelism)" in {
      //#init-db-config-session
      val databaseConfig = DatabaseConfig.forConfig[JdbcProfile]("slick-h2")
      implicit val session = SlickSession.forConfig(databaseConfig)
      //#init-db-config-session

      val inserted = Source(users)
        .via(Slick.flow(insertUser))
        .runWith(Sink.seq)
        .futureValue

      inserted must have size (users.size)
      inserted.toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table (parallelism = 4)" in {
      val inserted = Source(users)
        .via(Slick.flow(parallelism = 4, insertUser))
        .runWith(Sink.seq)
        .futureValue

      inserted must have size (users.size)
      inserted.toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table faster using Flow.grouped (n = 10, parallelism = 4)" in {
      val inserted = Source(users)
        .grouped(10)
        .via(
          Slick.flow(parallelism = 4, (group: Seq[User]) => group.map(insertUser(_)).reduceLeft(_.andThen(_)))
        )
        .runWith(Sink.seq)
        .futureValue

      inserted must have size 4
      // we do single inserts without auto-commit but it only returns the result of the last insert
      inserted.toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }
  }

  "Slick.flowWithPassThrough(..)" must {
    "insert 40 records into a table (no parallelism)" in {
      val inserted = Source(users)
        .via(Slick.flowWithPassThrough { user =>
          insertUser(user).map(insertCount => (user, insertCount))
        })
        .runWith(Sink.seq)
        .futureValue

      inserted must have size (users.size)
      inserted.map(_._1).toSet mustBe users
      inserted.map(_._2).toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table (parallelism = 4)" in {
      val inserted = Source(users)
        .via(
          Slick.flowWithPassThrough(parallelism = 4,
                                    user => {
                                      insertUser(user).map(insertCount => (user, insertCount))
                                    }
          )
        )
        .runWith(Sink.seq)
        .futureValue

      inserted must have size (users.size)
      inserted.map(_._1).toSet mustBe users
      inserted.map(_._2).toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table faster using Flow.grouped (n = 10, parallelism = 4)" in {
      val inserted = Source(users)
        .grouped(10)
        .via(
          Slick.flowWithPassThrough(
            parallelism = 4,
            (group: Seq[User]) => {
              val groupedDbActions = group.map(user => insertUser(user).map(insertCount => Seq((user, insertCount))))
              DBIOAction.fold(groupedDbActions, Seq.empty[(User, Int)])(_ ++ _)
            }
          )
        )
        .runWith(Sink.fold(Seq.empty[(User, Int)])((a, b) => a ++ b))
        .futureValue

      inserted must have size (users.size)
      inserted.map(_._1).toSet mustBe users
      inserted.map(_._2).toSet mustBe Set(1)

      getAllUsersFromDb.futureValue mustBe users
    }

    "kafka-example - store documents and pass Responses with passThrough" in {

      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to a db with Slick, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage[A](msg: A, offset: KafkaOffset) {
        // map the msg and keep the offset
        def map[B](f: A => B): KafkaMessage[B] = KafkaMessage(f(msg), offset)
      }

      val messagesFromKafka = users.zipWithIndex.map { case (user, index) => KafkaMessage(user, KafkaOffset(index)) }

      var committedOffsets = List[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Future[Done] = {
        committedOffsets = committedOffsets :+ offset
        Future.successful(Done)
      }

      val f1 = Source(messagesFromKafka) // Assume we get this from Kafka
        .via( // write to db with Slick
          Slick.flowWithPassThrough { kafkaMessage =>
            insertUser(kafkaMessage.msg).map(insertCount => kafkaMessage.map(_ => insertCount))
          }
        )
        .mapAsync(1) { kafkaMessage =>
          if (kafkaMessage.msg == 0) throw new Exception("Failed to write message to db")
          // Commit to kafka
          commitToKafka(kafkaMessage.offset)
        }
        .runWith(Sink.seq)

      Await.ready(f1, Duration.Inf)

      // Make sure all messages was committed to kafka
      committedOffsets.map(_.offset).sorted mustBe ((0 until (users.size)).toList)

      // Assert that all docs were written to db
      getAllUsersFromDb.futureValue mustBe users
    }
  }

  "Slick.sink(..)" must {
    "insert 40 records into a table (no parallelism)" in {
      Source(users)
        .runWith(Slick.sink(insertUser))
        .futureValue

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table (parallelism = 4)" in {
      Source(users)
        .runWith(Slick.sink(parallelism = 4, insertUser))
        .futureValue

      getAllUsersFromDb.futureValue mustBe users
    }

    "insert 40 records into a table faster using Flow.grouped (n = 10, parallelism = 4)" in {
      Source(users)
        .grouped(10)
        .runWith(
          Slick.sink(parallelism = 4, (group: Seq[User]) => group.map(insertUser(_)).reduceLeft(_.andThen(_)))
        )
        .futureValue

      getAllUsersFromDb.futureValue mustBe users
    }
  }
}
