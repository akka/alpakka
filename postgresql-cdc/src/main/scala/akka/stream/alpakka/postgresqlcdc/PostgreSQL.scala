/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager, PreparedStatement}

import akka.annotation.InternalApi
import akka.event.LoggingAdapter

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

@InternalApi private[postgresqlcdc] object PostgreSQL {

  def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  /** Checks that the slot exists */
  def checkSlotExists(slotName: String, plugin: Plugin)(implicit conn: Connection, log: LoggingAdapter): Boolean = {

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
        case plugin.name =>
          log.info("found logical replication slot with name {} for database {} using {} plugin",
                   slotName,
                   database,
                   plugin.name)
        case _ =>
          log.warning("improper plugin configuration for slot with name {}", slotName)
      }
      true
    }
  }

  def createSlot(slotName: String, plugin: Plugin)(implicit conn: Connection, log: LoggingAdapter): Unit = {
    log.info("setting up logical replication slot {}", slotName)
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot('$slotName','${plugin.name}')")
    stmt.execute()
  }

  def buildGetSlotChangesStatement(slotName: String, maxItems: Int)(implicit conn: Connection): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  def buildPeekSlotChangesStatement(slotName: String, maxItems: Int)(implicit conn: Connection): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_peek_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  def flush(slotName: String, upToLsn: String)(implicit conn: Connection): Unit = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_get_changes(?,?, NULL) LIMIT 0")
    statement.setString(1, slotName)
    statement.setString(2, upToLsn)
    statement.execute()
    statement.close()
  }

  def pullChanges(mode: Mode, slotName: String, maxItems: Int)(implicit conn: Connection): List[SlotChange] = {
    val pullChangesStatement = mode match {
      case Modes.Get => buildGetSlotChangesStatement(slotName, maxItems)
      case Modes.Peek => buildPeekSlotChangesStatement(slotName, maxItems)
    }
    val rs = pullChangesStatement.executeQuery()
    val result = ArrayBuffer[SlotChange]()
    while (rs.next()) {
      val data = rs.getString("data")
      val transactionId = rs.getLong("xid")
      val location = Try(rs.getString("location")).getOrElse(rs.getString("lsn"))
      result += SlotChange(transactionId, location, data)
    }
    pullChangesStatement.close()
    result.toList
  }

  /** Represents a row in the table we get from PostgreSQL when we query
   * SELECT * FROM pg_logical_slot_get_changes(..)
   */
  case class SlotChange(transactionId: Long, location: String, data: String)

}
