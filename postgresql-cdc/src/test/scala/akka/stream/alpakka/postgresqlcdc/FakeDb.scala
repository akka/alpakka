/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager}
import java.time.Instant

import org.postgresql.util.PGobject

/* for testing */
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
          |  time TIMESTAMPTZ,
          |  PRIMARY KEY(id)
          |);
        """.stripMargin)

    createStatement.execute()
    createStatement.close()
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
    createStatement.close()
  }

  def createPurchaseOrdersTable()(implicit conn: Connection): Unit = {
    val st = conn.prepareStatement("""
        |CREATE TABLE purchase_orders (
        | id SERIAL NOT NULL PRIMARY KEY,
        | info XML NOT NULL
        | );
      """.stripMargin)
    st.execute()
    st.close()
  }

  def createEmployeesTable()(implicit conn: Connection): Unit = {
    val st = conn.prepareStatement("""
        |CREATE TABLE employees (
        | id serial NOT NULL PRIMARY KEY,
        | name VARCHAR(255) NOT NULL,
        | position VARCHAR(255) DEFAULT NULL
        |);
        |
      """.stripMargin)
    st.execute()
    st.close()
  }

  def createImagesTable()(implicit conn: Connection): Unit = {
    val st = conn.prepareStatement("""
        |CREATE TABLE images(
        | id serial NOT NULL PRIMARY KEY,
        | image BYTEA NOT NULL
        |);
      """.stripMargin)
    st.execute()
    st.close()
  }

  def createWeatherTable()(implicit conn: Connection): Unit = {
    val createTableSt = conn.prepareStatement("""
        |CREATE TABLE "WEATHER"(
        | id serial NOT NULL PRIMARY KEY,
        | city VARCHAR(255) NOT NULL,
        | weather VARCHAR(255) NOT NULL
        |);
      """.stripMargin)
    createTableSt.execute()
    createTableSt.close()

    val alterTableSt = conn.prepareStatement("ALTER TABLE \"WEATHER\" REPLICA IDENTITY FULL")
    alterTableSt.execute()
    alterTableSt.close()
  }

  def dropTable(name: String)(implicit conn: Connection): Unit = {
    val st = conn.prepareCall(s"DROP TABLE $name")
    st.execute()
    st.close()
  }

  def dropTableCustomers()(implicit conn: Connection): Unit = dropTable("customers")

  def dropTableSales()(implicit conn: Connection): Unit = dropTable("sales")

  def dropTablePurchaseOrders()(implicit conn: Connection): Unit = dropTable("purchase_orders")

  def dropTableEmployees()(implicit conn: Connection): Unit = dropTable("employees")

  def dropTableImages()(implicit conn: Connection): Unit = dropTable("images")

  def dropTableWeather()(implicit conn: Connection): Unit = dropTable(""""WEATHER"""")

  // for the Java DSL test
  def insertCustomer(id: Int, fName: String, lName: String, email: String, tags: java.util.List[String], time: Instant)(
      implicit conn: Connection
  ): Unit = {
    import scala.collection.JavaConverters._
    insertCustomer(id, fName, lName, email, tags.asScala.toList, time)(conn)
  }

  def insertCustomer(id: Int, fName: String, lName: String, email: String, tags: List[String], time: Instant)(
      implicit conn: Connection
  ): Unit = {
    val insertStatement =
      conn.prepareStatement(
        "INSERT INTO customers(id, first_name, last_name, email, tags, time) VALUES(?, ?, ?, ?, ?, ?)"
      )
    insertStatement.setInt(1, id)
    insertStatement.setString(2, fName)
    insertStatement.setString(3, lName)
    insertStatement.setString(4, email)
    insertStatement.setArray(5, conn.createArrayOf("text", tags.toArray))
    insertStatement.setTimestamp(6, new java.sql.Timestamp(time.toEpochMilli))
    insertStatement.execute()
    insertStatement.close()
  }

  def updateCustomerEmail(id: Int, newEmail: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE customers SET email = ? WHERE Id = ?")
    updateStatement.setString(1, newEmail)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteCustomers()(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM customers")
    deleteStatement.execute()
    deleteStatement.close()
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
    insertStatement.close()
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
    updateStatement.close()
  }

  def deleteSale(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM sales WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
    deleteStatement.close()
  }

  def insertPurchaseOrder(id: Int, info: String)(implicit conn: Connection): Unit = {
    val pGobject = new PGobject
    pGobject.setType("XML")
    pGobject.setValue(info)
    val insertStatement = conn.prepareStatement("INSERT INTO purchase_orders(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pGobject)
    insertStatement.execute()
    insertStatement.close()
  }

  def deletePurchaseOrder(id: Int)(implicit conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM purchase_orders WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
    deleteStatement.close()
  }

  def insertEmployee(id: Int, name: String, position: String)(implicit conn: Connection): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO employees(id, name, position) VALUES(?, ?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, name)
    insertStatement.setString(3, position)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateEmployee(id: Int, newPosition: String)(implicit conn: Connection): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE employees SET position = ? WHERE id = ?;")
    updateStatement.setString(1, newPosition)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteEmployees()(implicit conn: Connection): Unit = {
    val st = conn.prepareStatement("DELETE FROM employees;")
    st.execute()
    st.close()
  }

  def insertImage(id: Int, imageName: String)(implicit conn: Connection): Unit = {
    val fis = FakeDb.getClass.getResourceAsStream(imageName)
    assert(fis != null)
    val insertStatement =
      conn.prepareStatement("INSERT INTO images(id, image) VALUES(?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setBinaryStream(2, fis)
    insertStatement.execute()
    insertStatement.close()
    fis.close()
  }

  def deleteImages()(implicit conn: Connection): Unit = {
    val deleteSt = conn.prepareStatement("DELETE FROM images;")
    deleteSt.execute()
    deleteSt.close()
  }

  def insertWeather(id: Int, city: String, weather: String)(implicit conn: Connection): Unit = {
    val insertStatement = conn.prepareStatement("INSERT INTO \"WEATHER\"(id, city, weather) VALUES(?, ?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, city)
    insertStatement.setString(3, weather)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateWeather(id: Int, newWeather: String)(implicit conn: Connection): Unit = {
    val updateStatement = conn.prepareStatement("UPDATE \"WEATHER\" SET weather = ? WHERE id = ?")
    updateStatement.setString(1, newWeather)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteWeathers()(implicit conn: Connection): Unit = {
    val deleteSt = conn.prepareStatement("DELETE FROM \"WEATHER\";")
    deleteSt.execute()
    deleteSt.close()
  }

  def setUpLogicalDecodingSlot(slotName: String)(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot('${slotName}','test_decoding')")
    stmt.execute()
    stmt.close()
  }

  def dropLogicalDecodingSlot(slotName: String)(implicit conn: Connection): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_drop_replication_slot('${slotName}')")
    stmt.execute()
    stmt.close()
  }

}
