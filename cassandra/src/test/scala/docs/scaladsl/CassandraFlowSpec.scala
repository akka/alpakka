/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.{CassandraSessionSettings, CassandraWriteSettings}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSession, CassandraSource, CassandraSpecBase}
import akka.stream.scaladsl.{Sink, Source, SourceWithContext}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.collection.immutable
import scala.concurrent.Future

class CassandraFlowSpec extends CassandraSpecBase(ActorSystem("CassandraFlowSpec")) {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
  val data = 1 until 103

  override val lifecycleSession: CassandraSession = sessionRegistry.sessionFor(sessionSettings)

  "CassandraFlow" must {
    implicit val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings)

    "update with simple prepared statement" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
      }.futureValue mustBe Done

      val written: Future[Done] = Source(data)
        .via(
          CassandraFlow.create(CassandraWriteSettings.defaults,
                               s"INSERT INTO $table(id) VALUES (?)",
                               (element, preparedStatement) => preparedStatement.bind(Int.box(element)))
        )
        .runWith(Sink.ignore)

      written.futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table").runWith(Sink.seq).futureValue
      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "update with prepared statement" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY,
                         |    name text,
                         |    city text
                         |);""".stripMargin)
      }.futureValue mustBe Done

      // #prepared
      import akka.stream.alpakka.cassandra.CassandraWriteSettings
      import akka.stream.alpakka.cassandra.scaladsl.CassandraFlow
      import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}

      case class Person(id: Int, name: String, city: String)

      val persons =
        immutable.Seq(Person(12, "John", "London"), Person(43, "Umberto", "Roma"), Person(56, "James", "Chicago"))

      val statementBinder: (Person, PreparedStatement) => BoundStatement =
        (person, preparedStatement) => preparedStatement.bind(Int.box(person.id), person.name, person.city)

      val written: Future[immutable.Seq[Person]] = Source(persons)
        .via(
          CassandraFlow.create(CassandraWriteSettings.defaults,
                               s"INSERT INTO $table(id, name, city) VALUES (?, ?, ?)",
                               statementBinder)
        )
        .runWith(Sink.seq)
      // #prepared

      written.futureValue must have size (persons.size)

      val rows = CassandraSource(s"SELECT * FROM $table")
        .map { row =>
          Person(row.getInt("id"), row.getString("name"), row.getString("city"))
        }
        .runWith(Sink.seq)
        .futureValue
      rows must contain theSameElementsAs persons
    }

    "with context usage" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY,
                         |    name text,
                         |    city text
                         |);""".stripMargin)
      }.futureValue mustBe Done

      case class Person(id: Int, name: String, city: String)
      case class AckHandle(id: Int) {
        def ack(): Future[Done] = Future.successful(Done)
      }
      val persons =
        immutable.Seq(Person(12, "John", "London") -> AckHandle(12),
                      Person(43, "Umberto", "Roma") -> AckHandle(43),
                      Person(56, "James", "Chicago") -> AckHandle(56))

      // #withContext
      val personsAndHandles: SourceWithContext[Person, AckHandle, NotUsed] = // ???
        // #withContext
        SourceWithContext.fromTuples(Source(persons))
      // #withContext

      val written: Future[Done] = personsAndHandles
        .via(
          CassandraFlow.withContext(
            CassandraWriteSettings.defaults,
            s"INSERT INTO $table(id, name, city) VALUES (?, ?, ?)",
            (person, preparedStatement) => preparedStatement.bind(Int.box(person.id), person.name, person.city)
          )
        )
        .asSource
        .mapAsync(1) {
          case (_, handle) => handle.ack()
        }
        .runWith(Sink.ignore)
      // #withContext

      written.futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table")
        .map { row =>
          Person(row.getInt("id"), row.getString("name"), row.getString("city"))
        }
        .runWith(Sink.seq)
        .futureValue
      rows must contain theSameElementsAs persons.map(_._1)
    }

    "allow batches" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY,
                         |    name text,
                         |    city text
                         |);""".stripMargin)
      }.futureValue mustBe Done

      case class Person(id: Int, name: String, city: String)

      val persons =
        immutable.Seq(Person(12, "John", "London"), Person(43, "Umberto", "Roma"), Person(56, "James", "Chicago"))
      val written = Source(persons)
        .via(
          CassandraFlow.createBatch(
            CassandraWriteSettings.defaults,
            s"INSERT INTO $table(id, name, city) VALUES (?, ?, ?)",
            statementBinder =
              (person, preparedStatement) => preparedStatement.bind(Int.box(person.id), person.name, person.city),
            groupingKey = person => person.id
          )
        )
        .runWith(Sink.ignore)
      written.futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table")
        .map { row =>
          Person(row.getInt("id"), row.getString("name"), row.getString("city"))
        }
        .runWith(Sink.seq)
        .futureValue
      rows must contain theSameElementsAs persons
    }
  }
}
