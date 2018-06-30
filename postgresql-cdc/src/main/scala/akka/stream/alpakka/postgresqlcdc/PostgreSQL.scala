/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager, PreparedStatement}

import akka.annotation.InternalApi
import akka.event.LoggingAdapter

import scala.collection.mutable.ArrayBuffer

@InternalApi private[postgresqlcdc] object PostgreSQL {

  def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  /** Checks that the slot exists */
  def checkSlotExists(slotName: String)(implicit conn: Connection, log: LoggingAdapter): Boolean = {

    val getReplicationSlots = conn.prepareStatement(
      "SELECT * FROM pg_replication_slots WHERE slot_name = ?"
    )
    getReplicationSlots.setString(1, slotName)
    val rs = getReplicationSlots.executeQuery()

    if (!rs.next()) {
      log.info("logical replication slot with name {} does not exist", slotName)
      false
    } else {
      val database = rs.getString("database")
      val foundPlugin = rs.getString("plugin")
      foundPlugin match {
        case "test_decoding" =>
          log.info("found logical replication slot with name {} for database {} using test_decoding plugin",
                   slotName,
                   database)
        case _ =>
          log.warning("improper plugin configuration for slot with name {}", slotName)
      }
      true
    }
  }

  def setUpSlot(slotName: String)(implicit conn: Connection, log: LoggingAdapter): Unit = {
    log.info("setting up logical replication slot {}", slotName)
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot('$slotName','test_decoding')")
    stmt.execute()
  }

  def buildGetSlotChangesStmt(slotName: String, maxItems: Int)(implicit conn: Connection): PreparedStatement = {
    val getSlotChangesStmt: PreparedStatement =
      conn.prepareStatement(
        "SELECT * FROM pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')"
      )
    getSlotChangesStmt.setString(1, slotName)
    getSlotChangesStmt.setInt(2, maxItems)
    getSlotChangesStmt
  }

  def getSlotChanges(getSlotChangesStmt: PreparedStatement): List[SlotChange] = {
    val rs = getSlotChangesStmt.executeQuery()
    val result = ArrayBuffer[SlotChange]()
    while (rs.next()) {
      val data = rs.getString("data")
      val transactionId = rs.getLong("xid")
      result += SlotChange(transactionId, data)
    }
    result.toList
  }

  /** Represents a row in the table we get from PostgreSQL when we query
   * SELECT * FROM pg_logical_slot_get_changes(..)
   */
  case class SlotChange(transactionId: Long, data: String)

}
