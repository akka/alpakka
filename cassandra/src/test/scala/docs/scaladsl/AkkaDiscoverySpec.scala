/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSessionRegistry, CassandraSpecBase}
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.concurrent.Future

class AkkaDiscoverySpec extends CassandraSpecBase(ActorSystem("AkkaDiscoverySpec")) {

  val sessionSettings = CassandraSessionSettings("with-akka-discovery")
  val data = 1 until 103

  override val lifecycleSession: CassandraSession = sessionRegistry.sessionFor(sessionSettings)

  "Service discovery" must {

    "connect to Cassandra" in assertAllStagesStopped {
      val session = sessionRegistry.sessionFor(sessionSettings)
      val table = createTableName()
      withSchemaMetadataDisabled {
        for {
          _ <- lifecycleSession.executeDDL(s"""
                                              |CREATE TABLE IF NOT EXISTS $table (
                                              |    id int PRIMARY KEY
                                              |);""".stripMargin)
          _ <- Future.sequence(data.map { i =>
            lifecycleSession.executeWrite(s"INSERT INTO $table(id) VALUES ($i)")
          })
        } yield Done
      }.futureValue mustBe Done
      val rows = session.select(s"SELECT * FROM $table").map(_.getInt("id")).runWith(Sink.seq).futureValue
      rows must contain theSameElementsAs data
    }

    "fail when the contact point address is invalid" in assertAllStagesStopped {
      val sessionSettings = CassandraSessionSettings("without-akka-discovery")
      val session = sessionRegistry.sessionFor(sessionSettings)
      val result = session.select(s"SELECT * FROM fsdfsd").runWith(Sink.head)
      val exception = result.failed.futureValue
      exception mustBe a[java.util.concurrent.CompletionException]
      exception.getCause mustBe a[com.datastax.oss.driver.api.core.AllNodesFailedException]
    }

    "fail when the port is missing" in assertAllStagesStopped {
      val sessionSettings = CassandraSessionSettings("with-akka-discovery-no-port")
      val session = sessionRegistry.sessionFor(sessionSettings)
      val result = session.select(s"SELECT * FROM fsdfsd").runWith(Sink.head)
      val exception = result.failed.futureValue
      exception mustBe a[akka.ConfigurationException]
    }

    "show referencing config in docs" in {
      // #discovery
      val sessionSettings = CassandraSessionSettings("example-with-akka-discovery")
      implicit val session: CassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
      // #discovery
      session.close(system.dispatcher)
    }

  }
}
