/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager, PreparedStatement}

import akka.NotUsed
import akka.event.LoggingAdapter
import akka.stream.alpakka.postgresqlcdc.scaladsl._
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}

import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.util.control.NonFatal

private[postgresqlcdc] object PostgreSQLSourceStage {

  import TestDecodingPlugin._

  private def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  /** Checks that the slot exists. Not necessary, just for helping the user */
  private def checkSlotExists(conn: Connection, slotName: String)(implicit log: LoggingAdapter): Unit = {
    val getReplicationSlots = conn.prepareStatement(
      "SELECT * FROM pg_replication_slots WHERE slot_name = ?"
    )
    getReplicationSlots.setString(1, slotName)
    val rs = getReplicationSlots.executeQuery()
    if (rs.next()) {
      val database = rs.getString("database")
      val plugin = rs.getString("plugin")
      plugin match {
        case "test_decoding" =>
          log.info("found replication slot with name {} for database {}", slotName, database)
        case _ =>
          log.warning("please use the test_decoding plugin for replication slot with name {}", slotName)
      }
    } else {
      log.warning("replication slot with name {} does not exist", slotName)
    }
  }

  private def buildGetSlotChangesStmt(conn: Connection, slotName: String, maxItems: Int): PreparedStatement = {
    val getSlotChangesStmt: PreparedStatement =
      conn.prepareStatement(
        "SELECT * FROM pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')"
      )
    getSlotChangesStmt.setString(1, slotName)
    getSlotChangesStmt.setInt(2, maxItems)
    getSlotChangesStmt
  }

  private def getSlotChanges(getSlotChangesStmt: PreparedStatement): List[SlotChange] = {
    val rs = getSlotChangesStmt.executeQuery()
    val result = ArrayBuffer[SlotChange]()
    while (rs.next()) {
      val data = rs.getString("data")
      val transactionId = rs.getLong("xid")
      result += SlotChange(transactionId, data)
    }
    result.toList
  }

  private def transformSlotChanges(slotChanges: List[SlotChange]): List[ChangeSet] = {

    slotChanges.groupBy(_.transactionId).map {

      case (transactionId: Long, slotChanges: List[SlotChange]) =>
        val changes: List[Change] = slotChanges.collect {

          case SlotChange(_, ChangeStatement(schemaName, tableName, "UPDATE", changes)) =>
            RowUpdated(schemaName, tableName, parseKeyValuePairs(changes))

          case SlotChange(_, ChangeStatement(schemaName, tableName, "DELETE", changes)) =>
            RowDeleted(schemaName, tableName, parseKeyValuePairs(changes))

          case SlotChange(_, ChangeStatement(schemaName, tableName, "INSERT", changes)) =>
            RowInserted(schemaName, tableName, parseKeyValuePairs(changes))
        }

        ChangeSet(transactionId, changes)

    }
  }.filter(_.changes.nonEmpty).toList.sortBy(_.transactionId)

  /** Represents a row in the table we get from PostgreSQL when we query
   * SELECT * FROM pg_logical_slot_get_changes(..)
   */
  private case class SlotChange(transactionId: Long, data: String)

}

private[postgresqlcdc] class PostgreSQLSourceStage(settings: PostgreSQLInstance)
    extends GraphStage[SourceShape[ChangeSet]] {

  import PostgreSQLSourceStage._

  private val out: Outlet[ChangeSet] = Outlet[ChangeSet]("PostgreSQLCDC.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {

      private val buffer = new Queue[ChangeSet]()

      private lazy val conn: Connection = getConnection(settings.connectionString)

      private lazy val prepStmt: PreparedStatement =
        buildGetSlotChangesStmt(conn, slotName = settings.slotName, maxItems = settings.maxItems)

      override def onTimer(timerKey: Any): Unit =
        retrieveChanges()

      private def retrieveChanges(): Unit = {
        val result: List[ChangeSet] = transformSlotChanges(getSlotChanges(prepStmt))

        if (result.isEmpty) {
          if (isAvailable(out)) {
            scheduleOnce(NotUsed, settings.duration)
          }
        } else {
          buffer ++= result
          push(out, buffer.dequeue())

        }
      }

      override def preStart(): Unit = {
        super.preStart()
        checkSlotExists(conn, settings.slotName)(log)
      }

      override def postStop(): Unit = {
        try {
          conn.close()
          log.debug("closed connection")
        } catch {
          case NonFatal(e) =>
            log.error("failed to close connection", e)
        }
        super.postStop()
      }

      setHandler(out, new OutHandler {

        override def onPull(): Unit =
          if (buffer.nonEmpty)
            push(out, buffer.dequeue())
          else
            retrieveChanges()
      })
    }

  override def shape: SourceShape[ChangeSet] = SourceShape(out)

}
