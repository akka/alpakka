/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.Connection

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.postgresqlcdc.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Attributes}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

abstract class PostgreSQLCapturerSpec(postgreSQLPortNumber: Int)
    extends TestKit(ActorSystem())
    with WordSpecLike
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll {

  import FakeDb._

  private val log = Logging(system, classOf[PostgreSQLCapturerSpec])

  private val connectionString =
    s"jdbc:postgresql://localhost:$postgreSQLPortNumber/pgdb1?user=pguser&password=pguser"

  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  private implicit val conn: Connection = getConnection(connectionString)

  private val postgreSQLInstance = PostgreSQLInstance().withConnectionString(connectionString).withSlotName("scalatest")

  override def beforeAll(): Unit = {
    log.info("setting up logical decoding slot and creating customers table")
    setUpLogicalDecodingSlot("scalatest")
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
    dropLogicalDecodingSlot("scalatest")
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

      ChangeDataCapture(postgreSQLInstance)
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

      ChangeDataCapture(postgreSQLInstance)
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

      ChangeDataCapture(postgreSQLInstance)
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

      ChangeDataCapture(postgreSQLInstance)
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

// See docker-compose.yml
// 5432 - PostgreSQL 9.4
// 5433 - PostgreSQL 9.5
// 5434 - PostgreSQL 9.6
// 5435 - PostgreSQL 10.4

class ChangeDataCapturePostgreSQL94 extends PostgreSQLCapturerSpec(5432)

class ChangeDataCapturePostgreSQL95 extends PostgreSQLCapturerSpec(5433)

class ChangeDataCapturePostgreSQL96 extends PostgreSQLCapturerSpec(5434)

class ChangeDataCapturePostgreSQL104 extends PostgreSQLCapturerSpec(5435)
