/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.util

import scala.collection.JavaConverters._

sealed abstract class Change {
  val schemaName: String
  val tableName: String

  /**
   * Java API
   */
  def getSchemaName(): String = schemaName

  /**
   * Java API
   */
  def getTableName(): String = tableName

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
  def getColumnName(): String = columnName

  /**
   * Java API
   */
  def getColumnType(): String = columnType

  /**
   * Java API
   */
  def getValue(): String = value

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      columnName == that.columnName &&
      columnType == that.columnType &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(columnName, columnType, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Field(columnName=$columnName, columnType=$columnType, value=$value)"
}

object RowInserted {

  def unapply(arg: RowInserted): Option[(String, String, List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.fields))

  def apply(schemaName: String, tableName: String, fields: List[Field]): RowInserted =
    new RowInserted(schemaName, tableName, fields)

}

final class RowInserted private (val schemaName: String, val tableName: String, val fields: List[Field])
    extends Change {

  /**
   * Java API
   */
  def getFields(): java.util.List[Field] = fields.asJava

  override def equals(other: Any): Boolean = other match {
    case that: RowInserted =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fields == that.fields
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RowInserted(schemaName=$schemaName, tableName=$tableName, fields=$fields)"
}

object RowUpdated {

  def unapply(arg: RowUpdated): Option[(String, String, List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.fields))

  def apply(schemaName: String, tableName: String, fields: List[Field]): RowUpdated =
    new RowUpdated(schemaName, tableName, fields)

}

final class RowUpdated private (val schemaName: String, val tableName: String, val fields: List[Field]) extends Change {

  /**
   * Java API
   */
  def getFields(): java.util.List[Field] = fields.asJava

  override def equals(other: Any): Boolean = other match {
    case that: RowUpdated =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fields == that.fields
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RowUpdated($schemaName, $tableName, $fields, $hashCode)"
}

object RowDeleted {

  def unapply(arg: RowDeleted): Option[(String, String, List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.fields))

  def apply(schemaName: String, tableName: String, fields: List[Field]): RowDeleted =
    new RowDeleted(schemaName, tableName, fields)

}

final class RowDeleted private (val schemaName: String, val tableName: String, val fields: List[Field]) extends Change {

  /**
   * Java API
   */
  def getFields(): util.List[Field] =
    fields.asJava

  override def equals(other: Any): Boolean = other match {
    case that: RowDeleted =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fields == that.fields
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RowDeleted(schemaName=$schemaName, tableName=$tableName, fields=$fields)"
}

object ChangeSet {

  def unapply(arg: ChangeSet): Option[(Long, List[Change])] =
    Some((arg.transactionId, arg.changes))

  def apply(transactionId: Long, changes: List[Change]): ChangeSet = new ChangeSet(transactionId, changes)

}

final class ChangeSet private (val transactionId: Long, val changes: List[Change]) {

  /**
   * Java API
   */
  def getChanges(): java.util.List[Change] =
    changes.asJava

  override def equals(other: Any): Boolean = other match {
    case that: ChangeSet =>
      transactionId == that.transactionId &&
      changes == that.changes
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(transactionId, changes)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"ChangeSet(transactionId=$transactionId, changes=$changes)"
}
