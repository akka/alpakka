/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time.Instant

import scala.collection.JavaConverters._

sealed abstract class Change {

  val schemaName: String
  val tableName: String

  val commitLogSeqNum: String
  val transactionId: Long

  /**
   * Java API
   */
  def getSchemaName: String = schemaName

  /**
   * Java API
   */
  def getTableName: String = tableName

  /**
   * Java API
   */
  def getTransactionId: Long = transactionId

  /**
   * Java API
   *
   * Commit log sequence number you need for the Ack Sink.
   * Note that this is for the WHOLE transaction that this change is part of.
   */
  def getCommitLogSeqNum: String = commitLogSeqNum

}

object Field {

  def unapply(arg: Field): Option[(String, String, String)] =
    Some((arg.columnName, arg.columnType, arg.value))

  def apply(columnName: String, columnType: String, value: String) = new Field(columnName, columnType, value)

}

final class Field private (val columnName: String, val columnType: String, val value: String) {

  /**
   * Java API
   */
  def getColumnName: String = columnName

  /**
   * Java API
   */
  def getColumnType: String = columnType

  /**
   * Java API
   */
  def getValue: String = value

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: Field ⇒
      columnName == that.columnName &&
      columnType == that.columnType &&
      value == that.value
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(columnName, columnType, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString = s"Field(columnName=$columnName, columnType=$columnType, value=$value)"

}

object RowInserted {

  def unapply(arg: RowInserted): Option[(String, String, String, Long, List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.fields))

  def apply(schemaName: String,
            tableName: String,
            logSeqNum: String,
            transactionId: Long,
            fields: List[Field]): RowInserted =
    new RowInserted(schemaName, tableName, logSeqNum, transactionId, fields)

}

final class RowInserted private (val schemaName: String,
                                 val tableName: String,
                                 val commitLogSeqNum: String,
                                 val transactionId: Long,
                                 val fields: List[Field])
    extends Change {

  /**
   * Java API
   */
  def getFields: java.util.List[Field] = fields.asJava

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowInserted ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      fields == that.fields
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowInserted(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, fields=$fields)"

  def copy(fields: List[Field]): RowInserted =
    new RowInserted(schemaName, tableName, commitLogSeqNum, transactionId, fields)

}

object RowUpdated {

  def unapply(arg: RowUpdated): Some[(String, String, String, Long, List[Field], List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.fieldsNew, arg.fieldsOld))

  def apply(schemaName: String,
            tableName: String,
            commitLogSeqNum: String,
            transactionId: Long,
            fieldsNew: List[Field],
            fieldsOld: List[Field]): RowUpdated =
    new RowUpdated(schemaName, tableName, commitLogSeqNum, transactionId, fieldsNew, fieldsOld)

}

final class RowUpdated private (val schemaName: String,
                                val tableName: String,
                                val commitLogSeqNum: String,
                                val transactionId: Long,
                                val fieldsNew: List[Field],
                                val fieldsOld: List[Field])
    extends Change {

  /**
   * Java API
   */
  def getFieldsNew: java.util.List[Field] = fieldsNew.asJava

  /**
   * Java API
   */
  def getFieldsOld: java.util.List[Field] = fieldsOld.asJava

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowUpdated ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      fieldsNew == that.fieldsNew &&
      fieldsOld == that.fieldsOld
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, fieldsNew, fieldsOld)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowUpdated(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, fieldsNew=$fieldsNew, fieldsOld=$fieldsOld)"

  def copy(fieldsNew: List[Field], fieldsOld: List[Field]): RowUpdated =
    new RowUpdated(schemaName, tableName, commitLogSeqNum, transactionId, fieldsNew, fieldsOld)

}

object RowDeleted {

  def unapply(arg: RowDeleted): Option[(String, String, String, Long, List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.commitLogSeqNum, arg.transactionId, arg.fields))

  def apply(schemaName: String,
            tableName: String,
            logSeqNum: String,
            transactionId: Long,
            fields: List[Field]): RowDeleted =
    new RowDeleted(schemaName, tableName, logSeqNum, transactionId, fields)

}

final class RowDeleted private (val schemaName: String,
                                val tableName: String,
                                val commitLogSeqNum: String,
                                val transactionId: Long,
                                val fields: List[Field])
    extends Change {

  /**
   * Java API
   */
  def getFields: java.util.List[Field] =
    fields.asJava

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowDeleted ⇒
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      commitLogSeqNum == that.commitLogSeqNum &&
      transactionId == that.transactionId &&
      fields == that.fields
    case _ ⇒ false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, commitLogSeqNum, transactionId, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }

  def copy(fields: List[Field]): RowDeleted =
    new RowDeleted(schemaName, tableName, commitLogSeqNum, transactionId, fields)

  // auto-generated
  override def toString =
    s"RowDeleted(schemaName=$schemaName, tableName=$tableName, commitLogSeqNum=$commitLogSeqNum, transactionId=$transactionId, fields=$fields)"
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
  def getChanges: java.util.List[Change] =
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

  override def toString = s"AckLogSeqNum(logSeqNum=$logSeqNum)"

  /**
   * Java API
   */
  def getLogSeqNum(): String = logSeqNum

}
