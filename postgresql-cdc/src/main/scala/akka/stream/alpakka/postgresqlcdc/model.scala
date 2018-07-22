/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time.Instant

import scala.collection.JavaConverters._
import java.util.{List ⇒ JavaList}
import java.util.{Map ⇒ JavaMap}

sealed abstract class Change {

  val schemaName: String
  val tableName: String

  val commitLogSeqNum: String
  val transactionId: Long

  /** Java API */
  def getSchemaName: String = schemaName

  /** Java API */
  def getTableName: String = tableName

  /** Java API */
  def getTransactionId: Long = transactionId

  /**
   * Java API
   *
   * Gets the commit log sequence number that the Ack Sink requires.
   * Note that this is for the WHOLE transaction that this change is part of.
   */
  def getCommitLogSeqNum: String = commitLogSeqNum

}

object RowInserted {

  def unapply(arg: RowInserted): Option[(String, String, String, Long, Map[String, String])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.data))

  def apply(schemaName: String,
            tableName: String,
            logSeqNum: String,
            transactionId: Long,
            data: Map[String, String],
            schema: Map[String, String]): RowInserted =
    new RowInserted(schemaName, tableName, logSeqNum, transactionId, data, schema)

}

final class RowInserted private (val schemaName: String,
                                 val tableName: String,
                                 val commitLogSeqNum: String,
                                 val transactionId: Long,
                                 val data: Map[String, String],
                                 val schema: Map[String, String])
    extends Change {

  /**
   * Java API
   */
  def getData: JavaMap[String, String] = data.asJava

  /**
   * Java API
   */
  def getSchema: JavaMap[String, String] = schema.asJava

  def copy(data: Map[String, String]): RowInserted =
    new RowInserted(schemaName, tableName, commitLogSeqNum, transactionId, data, schema)

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowInserted ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      data == that.data &&
      schema == that.schema
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, data, schema)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowInserted(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, data=$data, schema=$schema)"

}

object RowUpdated {

  def unapply(arg: RowUpdated): Some[(String, String, String, Long, Map[String, String], Map[String, String])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.dataNew, arg.dataOld))

  def apply(schemaName: String,
            tableName: String,
            commitLogSeqNum: String,
            transactionId: Long,
            dataNew: Map[String, String],
            dataOld: Map[String, String],
            schemaNew: Map[String, String],
            schemaOld: Map[String, String]): RowUpdated =
    new RowUpdated(schemaName, tableName, commitLogSeqNum, transactionId, dataNew, dataOld, schemaNew, schemaOld)

}

final class RowUpdated private (val schemaName: String,
                                val tableName: String,
                                val commitLogSeqNum: String,
                                val transactionId: Long,
                                val dataNew: Map[String, String],
                                val dataOld: Map[String, String],
                                val schemaNew: Map[String, String],
                                val schemaOld: Map[String, String])
    extends Change {

  /** Java API */
  def getDataNew: JavaMap[String, String] = dataNew.asJava

  /** Java API */
  def getSchemaNew: JavaMap[String, String] = schemaNew.asJava

  /** Java API */
  def getDataOld: JavaMap[String, String] = dataOld.asJava

  /** Java API */
  def getSchemaOld: JavaMap[String, String] = schemaOld.asJava

  def copy(dataNew: Map[String, String], dataOld: Map[String, String]): RowUpdated =
    new RowUpdated(schemaName, tableName, commitLogSeqNum, transactionId, dataNew, dataOld, schemaNew, schemaOld)

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowUpdated ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      dataNew == that.dataNew &&
      dataOld == that.dataOld &&
      schemaNew == that.schemaNew &&
      schemaOld == that.schemaOld
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, dataNew, dataOld, schemaNew, schemaOld)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowUpdated(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, dataNew=$dataNew, dataOld=$dataOld, schemaNew=$schemaNew, schemaOld=$schemaOld)"
}

object RowDeleted {

  def unapply(arg: RowDeleted): Option[(String, String, String, Long, Map[String, String])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.data))

  def apply(schemaName: String,
            tableName: String,
            commitLogSeqNum: String,
            transactionId: Long,
            data: Map[String, String],
            schema: Map[String, String]): RowDeleted =
    new RowDeleted(schemaName, tableName, commitLogSeqNum, transactionId, data, schema)

}

final class RowDeleted private (val schemaName: String,
                                val tableName: String,
                                val commitLogSeqNum: String,
                                val transactionId: Long,
                                val data: Map[String, String],
                                val schema: Map[String, String])
    extends Change {

  /** Java API */
  def getData: JavaMap[String, String] = data.asJava

  /** Java API */
  def getSchema: JavaMap[String, String] = schema.asJava

  def copy(data: Map[String, String]): RowDeleted =
    new RowDeleted(schemaName, tableName, commitLogSeqNum, transactionId, data, schema)

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowDeleted ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      data == that.data &&
      schema == that.schema
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, data, schema)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowDeleted(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, data=$data, schema=$schema)"
}

object ChangeSet {

  def unapply(arg: ChangeSet): Option[(Long, String, Instant, List[Change])] =
    Some((arg.transactionId, arg.commitLogSeqNum, arg.instant, arg.changes))

  def apply(transactionId: Long, location: String, instant: Instant, changes: List[Change]): ChangeSet =
    new ChangeSet(transactionId, location, instant, changes)

}

final class ChangeSet private (val transactionId: Long,
                               val commitLogSeqNum: String,
                               val instant: Instant,
                               val changes: List[Change]) {

  /**
   * Java API
   *
   * Note that this never returns an empty list. All change sets with empty changes are filtered out.
   */
  def getChanges: JavaList[Change] =
    changes.asJava

  /**
   * Java API
   */
  def getInstant: Instant =
    instant

  /**
   * Java API
   *
   * Commit log sequence number you need for the Ack Sink.
   */
  def getCommitLogSeqNum: String = commitLogSeqNum

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: ChangeSet ⇒
      transactionId == that.transactionId &&
      commitLogSeqNum == that.commitLogSeqNum &&
      instant == that.instant &&
      changes == that.changes
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(transactionId, commitLogSeqNum, instant, changes)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"ChangeSet(transactionId=$transactionId, commitLogSeqNum=$commitLogSeqNum, instant=$instant, changes=$changes)"
}

object AckLogSeqNum {

  def apply(logSeqNum: String): AckLogSeqNum = new AckLogSeqNum(logSeqNum)

  /**
   * Java API
   */
  def create(logSeqNum: String): AckLogSeqNum = AckLogSeqNum(logSeqNum)

}

final class AckLogSeqNum private (val logSeqNum: String) {

  // auto-generated
  override def toString = s"AckLogSeqNum(logSeqNum=$logSeqNum)"

  /**
   * Java API
   */
  def getLogSeqNum(): String = logSeqNum

}
