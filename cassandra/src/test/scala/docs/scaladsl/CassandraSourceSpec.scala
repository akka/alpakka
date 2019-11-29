/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSink, CassandraSource}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.datastax.driver.core.{Cluster, PreparedStatement, SimpleStatement}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
class CassandraSourceSpec
    extends AnyWordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  implicit val ec = system.dispatcher

  //#init-session
  implicit val session = Cluster.builder
    .addContactPoint("127.0.0.1")
    .withPort(9042)
    .build
    .connect()
  //#init-session

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  var keyspaceName: String = _

  override def beforeEach(): Unit = {
    keyspaceName = s"akka${System.nanoTime()}"
    session.execute(
      s"""
        |CREATE KEYSPACE $keyspaceName WITH replication = {
        |  'class': 'SimpleStrategy',
        |  'replication_factor': '1'
        |};
      """.stripMargin
    )
    session.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $keyspaceName.test (
        |    id int PRIMARY KEY
        |);
      """.stripMargin
    )

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspaceName.test_batch (
         |	id int,
         |	cc int,
         |	PRIMARY KEY (id, cc)
         |);
      """.stripMargin
    )
  }

  override def afterEach(): Unit =
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspaceName;")

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)

  def populate() =
    (1 until 103).map { i =>
      session.execute(s"INSERT INTO $keyspaceName.test(id) VALUES ($i)")
      i
    }

  "CassandraSourceSpec" must {

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped {
      val data = populate()
      val stmt = new SimpleStatement(s"SELECT * FROM $keyspaceName.test").setFetchSize(200)

      val rows = CassandraSource(stmt).runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "stream the result of a Cassandra statement with several pages" in {
      val data = populate()

      //#statement
      val stmt = new SimpleStatement(s"SELECT * FROM $keyspaceName.test").setFetchSize(20)
      //#statement

      //#run-source
      val rows = CassandraSource(stmt).runWith(Sink.seq)
      //#run-source

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "support multiple materializations" in assertAllStagesStopped {
      val data = populate()
      val stmt = new SimpleStatement(s"SELECT * FROM $keyspaceName.test")

      val source = CassandraSource(stmt)

      source.runWith(Sink.seq).futureValue.map(_.getInt("id")) must contain theSameElementsAs data
      source.runWith(Sink.seq).futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "stream the result of Cassandra statement that results in no data" in assertAllStagesStopped {
      val stmt = new SimpleStatement(s"SELECT * FROM $keyspaceName.test")

      val rows = CassandraSource(stmt).runWith(Sink.seq).futureValue

      rows mustBe empty
    }

    "write to the table using the flow and emit the elements in order" in assertAllStagesStopped {
      val source = Source(0 to 10).map(i => i: Integer)

      //#prepared-statement-flow
      val preparedStatement = session.prepare(s"INSERT INTO $keyspaceName.test(id) VALUES (?)")
      //#prepared-statement-flow

      //#statement-binder-flow
      val statementBinder = (myInteger: Integer, statement: PreparedStatement) => statement.bind(myInteger)
      //#statement-binder-flow

      //#run-flow
      val flow = CassandraFlow.createWithPassThrough[Integer](parallelism = 2, preparedStatement, statementBinder)

      val result = source.via(flow).runWith(Sink.seq)
      //#run-flow

      val resultToAssert = result.futureValue
      val found = session.execute(s"select id from $keyspaceName.test").all().asScala.map(_.getInt("id"))

      resultToAssert mustBe (0 to 10).toList
      found.toSet mustBe (0 to 10).toSet
    }

    "write to the table using the batching flow emitting the elements in any order" in assertAllStagesStopped {
      val source = Source(0 to 100).map(i => ToInsert(i % 2, i))

      //#prepared-statement-batching-flow
      val preparedStatement = session.prepare(s"INSERT INTO $keyspaceName.test_batch(id, cc) VALUES (?, ?)")
      //#prepared-statement-batching-flow

      //#statement-binder-batching-flow
      val statementBinder =
        (elemToInsert: ToInsert, statement: PreparedStatement) => statement.bind(elemToInsert.id, elemToInsert.cc)
      //#statement-binder-batching-flow

      //#settings-batching-flow
      val settings: CassandraBatchSettings = CassandraBatchSettings()
      //#settings-batching-flow

      //#run-batching-flow
      val flow = CassandraFlow.createUnloggedBatchWithPassThrough[ToInsert, Integer](parallelism = 2,
                                                                                     preparedStatement,
                                                                                     statementBinder,
                                                                                     ti => ti.id,
                                                                                     settings)

      val result = source.via(flow).runWith(Sink.seq)
      //#run-batching-flow

      val resultToAssert = result.futureValue
      val found = session.execute(s"select cc from $keyspaceName.test_batch").all().asScala.map(_.getInt("cc"))

      resultToAssert.map(_.cc) must contain theSameElementsAs (0 to 100).toList
      found.toSet mustBe (0 to 100).toSet
    }

    "write to the table using the sink" in assertAllStagesStopped {
      val source = Source(0 to 10).map(i => i: Integer)

      //#prepared-statement
      val preparedStatement = session.prepare(s"INSERT INTO $keyspaceName.test(id) VALUES (?)")
      //#prepared-statement

      //#statement-binder
      val statementBinder = (myInteger: Integer, statement: PreparedStatement) => statement.bind(myInteger)
      //#statement-binder

      //#run-sink
      val sink = CassandraSink[Integer](parallelism = 2, preparedStatement, statementBinder)

      val result = source.runWith(sink)
      //#run-sink

      result.futureValue

      val found = session.execute(s"select id from $keyspaceName.test").all().asScala.map(_.getInt("id"))

      found.toSet mustBe (0 to 10).toSet
    }
  }
}
