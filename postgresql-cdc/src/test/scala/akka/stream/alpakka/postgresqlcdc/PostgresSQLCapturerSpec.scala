/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.postgresqlcdc.scaladsl.PostgreSQLCapturer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class PostgresSQLCapturerSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll {

  private val log = Logging(system, classOf[PostgresSQLCapturerSpec])

  private val connectionString = "jdbc:postgresql://localhost/pgdb?user=pguser&password=pguser"

  private implicit val materializer = ActorMaterializer()

  private implicit val conn: Connection = getConnection(connectionString)

  private def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  private def creatCustomersTable(implicit conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
          |CREATE TABLE customers (
          |  id SERIAL,
          |  first_name VARCHAR(255) NOT NULL,
          |  last_name VARCHAR(255) NOT NULL,
          |  email VARCHAR(255) NOT NULL,
          |  PRIMARY KEY(id)
          |);
        """.stripMargin)

    createStatement.execute()
  }

  private def dropTableCustomers()(implicit conn: Connection): Unit = {
    val dropStatement =
      conn.prepareStatement("""
          |DROP TABLE customers;
        """.stripMargin)
    dropStatement.execute()
  }

  private def insertCustomer(id: Int, fName: String, lName: String, email: String)(implicit conn: Connection): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO customers(id, first_name, last_name, email) VALUES(?, ?, ?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, fName)
    insertStatement.setString(3, lName)
    insertStatement.setString(4, email)
    insertStatement.execute()
  }

  private def setUpLogicalDecodingSlot()(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement("SELECT * FROM pg_create_logical_replication_slot('scalatest','test_decoding')")
    stmt.execute()
  }

  private def dropLogicalDecodingSlot()(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement("SELECT * FROM pg_drop_replication_slot('scalatest')")
    stmt.execute()
  }

  override def beforeAll(): Unit = {
    log.info("setting up logical decoding slot and creating customers table")
    setUpLogicalDecodingSlot()
    creatCustomersTable
    log.info("inserting data into customers table")
    insertCustomer(id = 0, fName = "J", lName = "L", email = "j.l@akka.io")
    insertCustomer(id = 1, fName = "G", lName = "H", email = "g.h@akka.io")
    insertCustomer(id = 2, fName = "P", lName = "M", email = "p.m@akka.io")
    insertCustomer(id = 3, fName = "R", lName = "S", email = "r.s@akka.io")

  }

  override def afterAll: Unit = {
    log.info("dropping logical decoding slot and dropping customers table")
    TestKit.shutdownActorSystem(system)

    // The following are useful for local testing but not necessary when running this test on proper CI (like Travis) since
    // the CI creates fresh docker containers and destroys them after the test is complete anyway.
    dropLogicalDecodingSlot()
    dropTableCustomers()

    conn.close()

  }

  "A PostgreSQL change data capture source" must {

    "capture changes to database tables" in {

      val settings = PostgreSQLChangeDataCaptureSettings(connectionString, slotName = "scalatest")

      PostgreSQLCapturer(settings)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(4)
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields))) =>
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields))) =>
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields))) =>
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields))) =>
        }
        .expectNoMessage(3 seconds)

    }

  }

}
