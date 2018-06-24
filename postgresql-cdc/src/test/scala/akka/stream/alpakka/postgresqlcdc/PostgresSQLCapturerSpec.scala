/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.postgresqlcdc.scaladsl.Plugins.TestDecoding
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.postgresqlcdc.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import org.postgresql.util.PGobject
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

abstract class PostgresSQLCapturerSpec(postgreSQLPortNumber: Int)
    extends TestKit(ActorSystem())
    with WordSpecLike
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll {

  private val log = Logging(system, classOf[PostgresSQLCapturerSpec])

  private val connectionString =
    s"jdbc:postgresql://localhost:${postgreSQLPortNumber}/pgdb1?user=pguser&password=pguser"

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
          |CREATE TABLE sales (
          | id SERIAL NOT NULL PRIMARY KEY,
          | info JSONB NOT NULL
          |);
        """.stripMargin)
    createStatement.execute()

  }

  private def createPurchaseOrdersTable()(implicit conn: Connection): Unit =
    conn.prepareStatement("""
      |CREATE TABLE purchase_orders (
      | id SERIAL NOT NULL PRIMARY KEY,
      | info XML NOT NULL
      | );
    """.stripMargin).execute()

  private def createEmployeesTable()(implicit conn: Connection): Unit =
    conn.prepareStatement("""
        |CREATE TABLE employees (
        | id serial NOT NULL PRIMARY KEY,
        | name VARCHAR(255) NOT NULL,
        | position VARCHAR(255) DEFAULT NULL
        |);
        |
      """.stripMargin).execute()

  private def dropTableCustomers()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE customers;").execute()

  private def dropTableSales()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE sales;").execute()

  private def dropTablePurchaseOrders()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE purchase_orders;").execute()

  private def dropTableEmployees()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE employees;").execute()

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

  private def insertPurchaseOrder(id: Int, info: String)(implicit conn: Connection): Unit = {
    val pGobject = new PGobject
    pGobject.setType("XML")
    pGobject.setValue(info)
    val insertStatement = conn.prepareStatement("INSERT INTO purchase_orders(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pGobject)
    insertStatement.execute()
  }

  private def deletePurchaseOrder(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM purchase_orders WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
  }

  private def insertEmployee(id: Int, name: String, position: String)(implicit conn: Connection): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO employees(id, name, position) VALUES(?, ?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, name)
    insertStatement.setString(3, position)
    insertStatement.execute()
  }

  private def updateEmployee(id: Int, newPosition: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE employees SET position = ? WHERE id = ?;")
    updateStatement.setString(1, newPosition)
    updateStatement.setInt(2, id)
    updateStatement.execute()
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
    createCustomersTable()
    createSalesTable()
    createPurchaseOrdersTable()
    createEmployeesTable()
  }

  override def afterAll: Unit = {
    log.info("dropping logical decoding slot and dropping customers table")
    TestKit.shutdownActorSystem(system)

    // The following are useful for local testing but not necessary when running this test on proper CI (like Travis) since
    // the CI creates fresh docker containers and destroys them after the test is complete anyway.
    dropLogicalDecodingSlot()
    dropTableCustomers()
    dropTableSales()
    dropTablePurchaseOrders()
    dropTableEmployees()

    conn.close()

  }

  "A PostgreSQL change data capture source" must {

    "capture changes to a table with numeric / character columns" in {

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

      ChangeDataCapture(PostgreSQLInstance(connectionString, slotName = "scalatest", TestDecoding))
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

    "capture changes to a table with jsonb columns" in {
      insertSale(id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(0)

      ChangeDataCapture(PostgreSQLInstance(connectionString, slotName = "scalatest", Plugins.TestDecoding))
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

    "capture changes to a table with xml columns" in {

      val xml = // from: https://msdn.microsoft.com/en-us/library/ms256129(v=vs.110).aspx
        """<?xml version="1.0"?>
          |<purchaseOrder xmlns="http://tempuri.org/po.xsd" orderDate="1999-10-20">
          |    <shipTo country="US">
          |        <name>Alice Smith</name>
          |        <street>123 Maple Street</street>
          |        <city>Mill Valley</city>
          |        <state>CA</state>
          |        <zip>90952</zip>
          |    </shipTo>
          |    <billTo country="US">
          |        <name>Robert Smith</name>
          |        <street>8 Oak Avenue</street>
          |        <city>Old Town</city>
          |        <state>PA</state>
          |        <zip>95819</zip>
          |    </billTo>
          |    <comment>Hurry, my lawn is going wild!</comment>
          |    <items>
          |        <item partNum="872-AA">
          |            <productName>Lawnmower</productName>
          |            <quantity>1</quantity>
          |            <USPrice>148.95</USPrice>
          |            <comment>Confirm this is electric</comment>
          |        </item>
          |        <item partNum="926-AA">
          |            <productName>Baby Monitor</productName>
          |            <quantity>1</quantity>
          |            <USPrice>39.98</USPrice>
          |            <shipDate>1999-05-21</shipDate>
          |        </item>
          |    </items>
          |</purchaseOrder>""".stripMargin

      insertPurchaseOrder(0, xml)

      deletePurchaseOrder(id = 0)

      ChangeDataCapture(PostgreSQLInstance(connectionString, slotName = "scalatest", Plugins.TestDecoding))
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(2)
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "purchase_orders", fields))) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowDeleted(_, _, _))) => // success
        }

    }

    "be able to deal with null columns" in {

      insertEmployee(0, "Giovanni", "employee")
      updateEmployee(0, null)

      ChangeDataCapture(PostgreSQLInstance(connectionString, slotName = "scalatest", Plugins.TestDecoding))
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(2)
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowInserted("public", "employees", _))) => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, List(RowUpdated("public", "employees", fields)))
              if fields.contains(Field("\"position\"", "character varying", "null")) => // success
        }

    }

  }

}

class ChangeDataCapturePostgresSQL94 extends PostgresSQLCapturerSpec(5432)

class ChangeDataCapturePostgreSQL104 extends PostgresSQLCapturerSpec(5433)
