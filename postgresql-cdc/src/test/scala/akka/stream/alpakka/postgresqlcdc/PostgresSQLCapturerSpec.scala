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
import org.postgresql.util.PGobject
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class PostgresSQLCapturerSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll {

  private val log = Logging(system, classOf[PostgresSQLCapturerSpec])

  private val connectionString = "jdbc:postgresql://localhost/pgdb?user=pguser&password=pguser"

  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  private implicit val conn: Connection = getConnection(connectionString)

  private def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  private def createCustomersTable()(implicit conn: Connection): Unit = {
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

  private def createSalesTable()(implicit conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
          |CREATE TABLE SALES(
          | id SERIAL NOT NULL PRIMARY KEY,
          | info JSONB NOT NULL
          |);
        """.stripMargin)
    createStatement.execute()

  }

  private def dropTableCustomers()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE customers;").execute()

  private def dropTableSales()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE sales;").execute()

  private def insertCustomer(id: Int, fName: String, lName: String, email: String)(implicit conn: Connection): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO customers(id, first_name, last_name, email) VALUES(?, ?, ?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, fName)
    insertStatement.setString(3, lName)
    insertStatement.setString(4, email)
    insertStatement.execute()
  }

  private def updateCustomerEmail(id: Int, newEmail: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE customers SET email = ? WHERE Id = ?")
    updateStatement.setString(1, newEmail)
    updateStatement.setInt(2, id)
    updateStatement.execute()
  }

  private def deleteCustomers()(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM customers")
    deleteStatement.execute()
  }

  private def insertSale(id: Int, info: String)(implicit conn: Connection): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(info)
    val insertStatement =
      conn.prepareStatement("INSERT INTO sales(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pgObject)
    insertStatement.execute()
  }

  private def updateSale(id: Int, newInfo: String)(implicit conn: Connection): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(newInfo)
    val updateStatement =
      conn.prepareStatement("UPDATE sales SET info = ? WHERE id = ?;")
    updateStatement.setObject(1, pgObject)
    updateStatement.setInt(2, id)
    updateStatement.execute()
  }

  private def deleteSale(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM sales WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
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
    createCustomersTable
    createSalesTable
  }

  override def afterAll: Unit = {
    log.info("dropping logical decoding slot and dropping customers table")
    TestKit.shutdownActorSystem(system)

    // The following are useful for local testing but not necessary when running this test on proper CI (like Travis) since
    // the CI creates fresh docker containers and destroys them after the test is complete anyway.
    dropLogicalDecodingSlot()
    dropTableCustomers()
    dropTableSales()

    conn.close()

  }

  "A PostgreSQL change data capture source" must {

    "capture changes to a table with numeric / character columns " in {

      log.info("inserting data into customers table")
      // some inserts
      insertCustomer(id = 0, fName = "John", lName = "Lennon", email = "john.lennon@akka.io")
      insertCustomer(id = 1, fName = "George", lName = "Harrison", email = "george.harrison@akka.io")
      insertCustomer(id = 2, fName = "Paul", lName = "McCartney", email = "paul.mccartney@akka.io")
      insertCustomer(id = 3, fName = "Ringo", lName = "Star", email = "ringo.star@akka.io")
      // some updates
      updateCustomerEmail(id = 0, "john.lennon@thebeatles.com")
      updateCustomerEmail(id = 1, "george.harrison@thebeatles.com")
      updateCustomerEmail(id = 2, "paul.mccartney@thebeatles.com")
      updateCustomerEmail(id = 3, "ringo.star@thebeatles.com")
      // some deletes
      deleteCustomers()

      val settings = PostgreSQLChangeDataCaptureSettings(connectionString, slotName = "scalatest")

      PostgreSQLCapturer(settings)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(9)
        //
        // expecting the insert events first
        //
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@akka.io"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@akka.io"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@akka.io"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "3",
                "first_name" -> "Ringo",
                "last_name" -> "Star",
                "email" -> "ringo.star@akka.io"
              ) => // success
        }
        //
        // expecting the update events next
        //
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@thebeatles.com"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@thebeatles.com"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@thebeatles.com"
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "customers", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "3",
                "first_name" -> "Ringo",
                "last_name" -> "Star",
                "email" -> "ringo.star@thebeatles.com"
              ) => // success
        }
        //
        //  expecting the delete events next (all in a single transaction )
        //
        .expectNextChainingPF {
          case c @ ChangeSet(_, deleteEvents)
              if deleteEvents.size == 4 && deleteEvents.count(_.isInstanceOf[RowDeleted]) == 4 => // success
        }

    }

    "capture changes to a table with a jsonb columns" in {
      insertSale(id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(0)

      val settings = PostgreSQLChangeDataCaptureSettings(connectionString, slotName = "scalatest")

      PostgreSQLCapturer(settings)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(3)
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "sales", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "0",
                "info" -> """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}"""
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "sales", fields)))
              if fields.map(f => f.columnName -> f.value).toSet == Set(
                "id" -> "0",
                "info" -> """{"name": "alpakka", "countries": ["*"]}"""
              ) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowDeleted(_, _, _))) => // success
        }

    }

  }

}
