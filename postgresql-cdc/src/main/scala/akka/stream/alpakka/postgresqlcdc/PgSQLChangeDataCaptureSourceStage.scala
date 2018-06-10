/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, DriverManager, PreparedStatement}

import akka.NotUsed
import akka.event.LoggingAdapter
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal
import scala.util.matching.Regex

/** Settings for PostgreSQL CDC
 *
 * @param connectionString PostgreSQL JDBC connection string
 * @param slotName         Name of the "logical decoding" slot
 * @param maxItems         Specifies how many rows are fetched in one batch
 * @param duration         Duration between polls
 */
final case class PostgreSQLChangeDataCaptureSettings(connectionString: String,
                                                     slotName: String,
                                                     maxItems: Int = 128,
                                                     duration: FiniteDuration = 2000 milliseconds)

sealed trait Change {
  val schemaName: String
  val tableName: String
}

case class Field(columnName: String, columnType: String, value: String)

case class RowInserted(schemaName: String, tableName: String, fields: Set[Field]) extends Change

case class RowUpdated(schemaName: String, tableName: String, fields: Set[Field]) extends Change

case class RowDeleted(schemaName: String, tableName: String, fields: Set[Field]) extends Change

case class ChangeSet(transactionId: Long, changes: Set[Change]) // TODO: add timestamp

private[postgresqlcdc] object PgSQLChangeDataCaptureSourceStage {

  import Grammar._

  private def getConnection(connectionString: String): Connection = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    DriverManager.getConnection(connectionString)
  }

  /** Checks that the slot exists. Not necessary, just for helping the user */
  private def checkSlotExists(conn: Connection, slotName: String)(implicit log: LoggingAdapter): Unit = {
    val getReplicationSlots = conn.prepareStatement(
      "SELECT * FROM pg_replication_slots WHERE " +
      "slot_name = ?"
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
        "SELECT * FROM " +
        "pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')"
      )
    getSlotChangesStmt.setString(1, slotName)
    getSlotChangesStmt.setInt(2, maxItems)
    getSlotChangesStmt
  }

  private def getSlotChanges(getSlotChangesStmt: PreparedStatement): Set[SlotChange] = {
    val rs = getSlotChangesStmt.executeQuery()
    val result = ArrayBuffer[SlotChange]()
    while (rs.next()) {
      val data = rs.getString("data")
      val location = rs.getString("location")
      val transactionId = rs.getLong("xid")
      result += SlotChange(transactionId, location, data)
    }
    result.toSet
  }

  private def transformSlotChanges(slotChanges: Set[SlotChange]): Set[ChangeSet] = {

    slotChanges.groupBy(_.transactionId).map {

      case (transactionId: Long, slotChanges: Set[SlotChange]) =>
        val changes: Set[Change] = slotChanges.collect {

          case SlotChange(_, _, ChangeStatement(schemaName, tableName, "UPDATE", properties)) =>
            RowUpdated(schemaName, tableName, parseKeyValuePairs(properties))

          case SlotChange(_, _, ChangeStatement(schemaName, tableName, "DELETE", properties)) =>
            RowDeleted(schemaName, tableName, parseKeyValuePairs(properties))

          case SlotChange(_, _, ChangeStatement(schemaName, tableName, "INSERT", properties)) =>
            RowInserted(schemaName, tableName, parseKeyValuePairs(properties))
        }

        ChangeSet(transactionId, changes)

    }
  }.filter(_.changes.nonEmpty).toSet

  private def parseKeyValuePairs(keyValuePairs: String): Set[Field] =
    KeyValuePair
      .findAllMatchIn(keyValuePairs)
      .collect {
        case regexMatch if regexMatch.groupCount == 3 =>
          // note that there is group 0 that denotes the entire match - and it is not included in the groupCount
          val columnName: String = regexMatch.group(1)
          val columnType: String = regexMatch.group(2)
          val value: String = regexMatch.group(3) match {
            case SingleQuotedString2(content) => content
            case other => other
          }
          Field(columnName, columnType, value)
      }
      .toSet

  /** Represents a row in the table we get from PostgreSQL when we query
   * SELECT * FROM pg_logical_slot_get_changes(..)
   */
  private case class SlotChange(transactionId: Long, location: String, data: String)

  private object Grammar {

    // We need to parse a log statement such as the following:
    //
    // table public.data: INSERT: id[integer]:3 data[text]:'3'
    //
    // Though it's complicated to parse it's not complicated enough to justify the cost of an additional dependency (could
    // have used FastParse or Scala Parser Combinators), hence we use standard library regular expressions.

    val Begin: Regex = "BEGIN (\\d+)".r

    val Commit
      : Regex = "COMMIT (\\d+) \\(at (\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2}\\.\\d+\\+\\d{2})\\)".r // matches
    // a commit message like COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)

    val DoubleQuotedString: String = "\"(?:\\\\\"|\"{2}|[^\"])+\"" // matches "Scala", "'Scala'"
    // or "The ""Scala"" language", "The \"Scala\" language" etc.

    val SingleQuotedString1: String = "'(?:\\\\'|'{2}|[^'])+'" // matches 'Scala', 'The ''Scala'' language' etc.

    val SingleQuotedString2: Regex = "'((?:\\\\'|'{2}|[^'])+)'".r

    val UnquotedIdentifier: String = "[^ \"'\\.]+"

    val Identifier: String = s"(?:$UnquotedIdentifier)|(?:$DoubleQuotedString)"

    val SchemaIdentifier: String = Identifier

    val TableIdentifier: String = Identifier

    val FieldIdentifier: String = Identifier

    val ChangeType: String = "\\bINSERT|\\bDELETE|\\bUPDATE"

    val TypeDeclaration: String = "[a-zA-Z0-9 ]+" // matches: [character varying] or [integer]

    val NonStringValue: String = "[^ \"']+" // matches: true, false, 3.14, 42 etc.

    val Value: String = s"(?:$NonStringValue)|(?:$SingleQuotedString1)" // matches: true, false, 3.14 or 'Strings can have spaces'

    val ChangeStatement: Regex =
      s"table ($SchemaIdentifier)\\.($TableIdentifier): ($ChangeType): (.+)".r

    val KeyValuePair: Regex = s"($FieldIdentifier)\\[($TypeDeclaration)\\]:($Value)".r

  }

}

private[postgresqlcdc] class PgSQLChangeDataCaptureSourceStage(settings: PostgreSQLChangeDataCaptureSettings)
    extends GraphStage[SourceShape[ChangeSet]] {

  import PgSQLChangeDataCaptureSourceStage._

  private val out: Outlet[ChangeSet] = Outlet[ChangeSet]("PostgreSQLCDC.out")

  override def shape: SourceShape[ChangeSet] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {

      private val buffer = new Queue[ChangeSet]()

      private lazy val conn: Connection = getConnection(settings.connectionString)

      private lazy val prepStmt: PreparedStatement =
        buildGetSlotChangesStmt(conn, slotName = settings.slotName, maxItems = settings.maxItems)

      override def onTimer(timerKey: Any): Unit =
        retrieveChanges()

      private def retrieveChanges(): Unit = {
        val result: Set[ChangeSet] = transformSlotChanges(getSlotChanges(prepStmt))

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

}
