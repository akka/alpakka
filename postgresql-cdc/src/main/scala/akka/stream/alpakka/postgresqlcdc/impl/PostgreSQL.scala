/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.impl

import java.sql.{Connection, DriverManager, PreparedStatement}

import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.stream.alpakka.postgresqlcdc.{Mode, Modes, Plugin}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi private[postgresqlcdc] object PostgreSQL {

  /** Represents a row in the table we get from PostgreSQL when we query
   * SELECT * FROM pg_logical_slot_get_changes(..)
   */
  case class SlotChange(transactionId: Long, location: String, data: String)

}

/**
 * INTERNAL API
 */
@InternalApi private[postgresqlcdc] trait PostgreSQL {

  import PostgreSQL._

  val conn: Connection
  def log: LoggingAdapter

  def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  /** Checks that the slot exists */
  def checkSlotExists(slotName: String, plugin: Plugin): Boolean = {

    val getReplicationSlots = conn.prepareStatement(
      "SELECT * FROM pg_replication_slots WHERE slot_name = ?"
    )
    getReplicationSlots.setString(1, slotName)
    val rs = getReplicationSlots.executeQuery()

    rs.next() match {
      case false ⇒
        log.info("logical replication slot with name {} does not exist", slotName)
        rs.close()
        false
      case true ⇒
        val database = rs.getString("database")
        val foundPlugin = rs.getString("plugin")
        foundPlugin match {
          case plugin.name ⇒
            log.info("found logical replication slot with name {} for database {} using {} plugin",
                     slotName,
                     database,
                     plugin.name)
          case _ ⇒
            log.warning("improper plugin configuration for slot with name {}", slotName)
        }
        rs.close()
        true
    }

  }

  def createSlot(slotName: String, plugin: Plugin): Unit = {
    log.info("setting up logical replication slot {}", slotName)
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot(?, ?)")
    stmt.setString(1, slotName)
    stmt.setString(2, plugin.name)
    stmt.execute()
  }

  def dropSlot(slotName: String): Unit = {
    log.info("dropping logical replication slot {}", slotName)
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_drop_logical_replication_slot(?)")
    stmt.setString(1, slotName)
    stmt.execute()
  }

  def buildGetSlotChangesStatement(slotName: String, maxItems: Int): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  def buildPeekSlotChangesStatement(slotName: String, maxItems: Int): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_peek_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  def flush(slotName: String, upToLogSeqNum: String): Unit = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT 1 FROM pg_logical_slot_get_changes(?,?, NULL)")
    statement.setString(1, slotName)
    statement.setString(2, upToLogSeqNum)
    statement.execute()
    statement.close()
  }

  def pullChanges(mode: Mode, slotName: String, maxItems: Int): List[SlotChange] = {
    val pullChangesStatement = mode match {
      case Modes.Get ⇒ buildGetSlotChangesStatement(slotName, maxItems)
      case Modes.Peek ⇒ buildPeekSlotChangesStatement(slotName, maxItems)
    }
    val rs = pullChangesStatement.executeQuery()
    val result = ArrayBuffer[SlotChange]()
    while (rs.next()) {
      val data = rs.getString("data")
      val transactionId = rs.getLong("xid")
      val location = Try(rs.getString("location"))
        .getOrElse(rs.getString("lsn")) // in older versions of PG the column is called "lsn" not "location"
      result += SlotChange(transactionId, location, data)
    }
    pullChangesStatement.close()
    result.toList
  }

}
