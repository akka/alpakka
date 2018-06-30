/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager}

import org.postgresql.util.PGobject

object FakeDb {

  def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  def createCustomersTable()(implicit conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
                              |CREATE TABLE customers (
                              |  id SERIAL,
                              |  first_name VARCHAR(255) NOT NULL,
                              |  last_name VARCHAR(255) NOT NULL,
                              |  email VARCHAR(255) NOT NULL,
                              |  tags TEXT[] NOT NULL,
                              |  PRIMARY KEY(id)
                              |);
                            """.stripMargin)

    createStatement.execute()
  }

  def createSalesTable()(implicit conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
                              |CREATE TABLE sales (
                              | id SERIAL NOT NULL PRIMARY KEY,
                              | info JSONB NOT NULL
                              |);
                            """.stripMargin)
    createStatement.execute()

  }

  def createPurchaseOrdersTable()(implicit conn: Connection): Unit =
    conn.prepareStatement("""
                            |CREATE TABLE purchase_orders (
                            | id SERIAL NOT NULL PRIMARY KEY,
                            | info XML NOT NULL
                            | );
                          """.stripMargin).execute()

  def createEmployeesTable()(implicit conn: Connection): Unit =
    conn.prepareStatement("""
                            |CREATE TABLE employees (
                            | id serial NOT NULL PRIMARY KEY,
                            | name VARCHAR(255) NOT NULL,
                            | position VARCHAR(255) DEFAULT NULL
                            |);
                            |
      """.stripMargin).execute()

  def dropTableCustomers()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE customers;").execute()

  def dropTableSales()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE sales;").execute()

  def dropTablePurchaseOrders()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE purchase_orders;").execute()

  def dropTableEmployees()(implicit conn: Connection): Unit =
    conn.prepareStatement("DROP TABLE employees;").execute()

  // for the Java DSL test
  def insertCustomer(id: Int, fName: String, lName: String, email: String, tags: java.util.List[String])(
      implicit conn: Connection
  ): Unit = {
    import scala.collection.JavaConverters._
    insertCustomer(id, fName, lName, email, tags.asScala.toList)(conn)
  }

  def insertCustomer(id: Int, fName: String, lName: String, email: String, tags: List[String])(
      implicit conn: Connection
  ): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO customers(id, first_name, last_name, email, tags) VALUES(?, ?, ?, ?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, fName)
    insertStatement.setString(3, lName)
    insertStatement.setString(4, email)
    insertStatement.setArray(5, conn.createArrayOf("text", tags.toArray))
    insertStatement.execute()
  }

  def updateCustomerEmail(id: Int, newEmail: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE customers SET email = ? WHERE Id = ?")
    updateStatement.setString(1, newEmail)
    updateStatement.setInt(2, id)
    updateStatement.execute()
  }

  def deleteCustomers()(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM customers")
    deleteStatement.execute()
  }

  def insertSale(id: Int, info: String)(implicit conn: Connection): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(info)
    val insertStatement =
      conn.prepareStatement("INSERT INTO sales(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pgObject)
    insertStatement.execute()
  }

  def updateSale(id: Int, newInfo: String)(implicit conn: Connection): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(newInfo)
    val updateStatement =
      conn.prepareStatement("UPDATE sales SET info = ? WHERE id = ?;")
    updateStatement.setObject(1, pgObject)
    updateStatement.setInt(2, id)
    updateStatement.execute()
  }

  def deleteSale(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM sales WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
  }

  def insertPurchaseOrder(id: Int, info: String)(implicit conn: Connection): Unit = {
    val pGobject = new PGobject
    pGobject.setType("XML")
    pGobject.setValue(info)
    val insertStatement = conn.prepareStatement("INSERT INTO purchase_orders(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pGobject)
    insertStatement.execute()
  }

  def deletePurchaseOrder(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM purchase_orders WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
  }

  def insertEmployee(id: Int, name: String, position: String)(implicit conn: Connection): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO employees(id, name, position) VALUES(?, ?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, name)
    insertStatement.setString(3, position)
    insertStatement.execute()
  }

  def updateEmployee(id: Int, newPosition: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE employees SET position = ? WHERE id = ?;")
    updateStatement.setString(1, newPosition)
    updateStatement.setInt(2, id)
    updateStatement.execute()
  }

  def deleteEmployees()(implicit conn: Connection): Unit =
    conn.prepareStatement("DELETE FROM employees;").execute()

  def setUpLogicalDecodingSlot(slotName: String)(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot('${slotName}','test_decoding')")
    stmt.execute()
  }

  def dropLogicalDecodingSlot(slotName: String)(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_drop_replication_slot('${slotName}')")
    stmt.execute()
  }

}
