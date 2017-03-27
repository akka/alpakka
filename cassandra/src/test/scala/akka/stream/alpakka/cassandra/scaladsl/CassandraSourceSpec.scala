/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.{Cluster, PreparedStatement, SimpleStatement}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
class CassandraSourceSpec
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-session
  implicit val session = Cluster.builder.addContactPoint("127.0.0.1").withPort(9042).build.connect()
  //#init-session

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  override def beforeEach(): Unit = {
    session.execute(
      """
        |CREATE KEYSPACE IF NOT EXISTS akka_stream_scala_test WITH replication = {
        |  'class': 'SimpleStrategy',
        |  'replication_factor': '1'
        |};
      """.stripMargin
    )
    session.execute(
      """
        |CREATE TABLE IF NOT EXISTS akka_stream_scala_test.test (
        |    id int PRIMARY KEY
        |);
      """.stripMargin
    )
  }

  override def afterEach(): Unit = {
    session.execute("DROP TABLE IF EXISTS akka_stream_scala_test.test;")
    session.execute("DROP KEYSPACE IF EXISTS akka_stream_scala_test;")
  }

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)

  def populate() =
    (1 until 103).map { i =>
      session.execute(s"INSERT INTO akka_stream_scala_test.test(id) VALUES ($i)")
      i
    }

  "CassandraSourceSpec" must {

    "stream the result of a Cassandra statement with one page" in {
      val data = populate()
      val stmt = new SimpleStatement("SELECT * FROM akka_stream_scala_test.test").setFetchSize(200)

      val rows = CassandraSource(stmt).runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "stream the result of a Cassandra statement with several pages" in {
      val data = populate()

      //#statement
      val stmt = new SimpleStatement("SELECT * FROM akka_stream_scala_test.test").setFetchSize(20)
      //#statement

      //#run-source
      val rows = CassandraSource(stmt).runWith(Sink.seq)
      //#run-source

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "support multiple materializations" in {
      val data = populate()
      val stmt = new SimpleStatement("SELECT * FROM akka_stream_scala_test.test")

      val source = CassandraSource(stmt)

      source.runWith(Sink.seq).futureValue.map(_.getInt("id")) must contain theSameElementsAs data
      source.runWith(Sink.seq).futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "stream the result of Cassandra statement that results in no data" in {
      val stmt = new SimpleStatement("SELECT * FROM akka_stream_scala_test.test")

      val rows = CassandraSource(stmt).runWith(Sink.seq).futureValue

      rows mustBe empty
    }

    "sink should write to the table" in {
      import system.dispatcher

      val source = Source(0 to 10).map(i => i: Integer)

      //#prepared-statement
      val preparedStatement = session.prepare("INSERT INTO akka_stream_scala_test.test(id) VALUES (?)")
      //#prepared-statement

      //#statement-binder
      val statementBinder = (myInteger: Integer, statement: PreparedStatement) => statement.bind(myInteger)
      //#statement-binder

      //#run-sink
      val sink = CassandraSink[Integer](parallelism = 2, preparedStatement, statementBinder)

      val result = source.runWith(sink)
      //#run-sink

      result.futureValue

      val found = session.execute("select id from akka_stream_scala_test.test").all().asScala.map(_.getInt("id"))

      found.toSet mustBe (0 to 10).toSet
    }
  }
}
