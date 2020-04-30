/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSource, CassandraSpecBase}
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.collection.immutable
import scala.concurrent.Future

class CassandraSourceSpec extends CassandraSpecBase(ActorSystem("CassandraSourceSpec")) {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  val sessionSettings = akka.stream.alpakka.cassandra.CassandraSessionSettings()
  val data = 1 until 103
  def intTable = keyspaceName + ".idtable"

  override val lifecycleSession: CassandraSession = sessionRegistry.sessionFor(sessionSettings)

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareIntTable(intTable)
  }

  "Retrieving a session" must {
    "be documented" in {
      // #init-session
      import akka.stream.alpakka.cassandra.CassandraSessionSettings
      import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
      import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry

      val system: ActorSystem = // ???
        // #init-session
        this.system
      // #init-session
      val sessionSettings = CassandraSessionSettings()
      implicit val cassandraSession: CassandraSession =
        CassandraSessionRegistry.get(system).sessionFor(sessionSettings)

      val version: Future[String] =
        cassandraSession
          .select("SELECT release_version FROM system.local;")
          .map(_.getString("release_version"))
          .runWith(Sink.head)
      // #init-session
      version.futureValue must not be empty
    }
  }

  "CassandraSourceSpec" must {
    implicit val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings)

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped {
      // #cql
      import akka.stream.alpakka.cassandra.scaladsl.CassandraSource

      val ids: Future[immutable.Seq[Int]] =
        CassandraSource(s"SELECT id FROM $intTable").map(row => row.getInt("id")).runWith(Sink.seq)

      // #cql
      ids.futureValue must contain theSameElementsAs data
    }

    "support parameters" in assertAllStagesStopped {
      val value: Integer = 5
      // #cql
      val idsWhere: Future[Int] =
        CassandraSource(s"SELECT * FROM $intTable WHERE id = ?", value).map(_.getInt("id")).runWith(Sink.head)
      // #cql
      idsWhere.futureValue mustBe value
    }

    "stream the result of a Cassandra statement with several pages" in assertAllStagesStopped {
      // #statement
      import com.datastax.oss.driver.api.core.cql.{Row, SimpleStatement}

      val stmt = SimpleStatement.newInstance(s"SELECT * FROM $intTable").setPageSize(20)

      val rows: Future[immutable.Seq[Row]] = CassandraSource(stmt).runWith(Sink.seq)
      // #statement

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "allow prepared statements" in assertAllStagesStopped {
      val stmt = session.prepare(s"SELECT * FROM $intTable").map(_.bind())
      val rows = CassandraSource.fromFuture(stmt).runWith(Sink.seq)

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

  }

  private def prepareIntTable(table: String) = {
    withSchemaMetadataDisabled {
      for {
        _ <- lifecycleSession.executeDDL(s"""
             |CREATE TABLE IF NOT EXISTS $table (
             |    id int PRIMARY KEY
             |);""".stripMargin)
        _ <- executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)"))
      } yield Done
    }.futureValue mustBe Done
  }
}
