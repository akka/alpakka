/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time.ZonedDateTime
import scala.collection.JavaConverters._

sealed abstract class Change {
  val schemaName: String
  val tableName: String

  /**
   * Java API
   */
  def getSchemaName: String = schemaName

  /**
   * Java API
   */
  def getTableName: String = tableName

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
    case that: Field =>
      columnName == that.columnName &&
      columnType == that.columnType &&
      value == that.value
    case _ => false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(columnName, columnType, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  // auto-generated
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
  def getFields: java.util.List[Field] = fields.asJava

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowInserted =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fields == that.fields
    case _ => false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  // auto-generated
  override def toString = s"RowInserted(schemaName=$schemaName, tableName=$tableName, fields=$fields)"
}

object RowUpdated {

  def unapply(arg: RowUpdated): Some[(String, String, List[Field], List[Field])] =
    Some((arg.schemaName, arg.tableName, arg.fieldsNew, arg.fieldsOld))

  def apply(schemaName: String, tableName: String, fieldsNew: List[Field], fieldsOld: List[Field]): RowUpdated =
    new RowUpdated(schemaName, tableName, fieldsNew, fieldsOld)

}

final class RowUpdated private (val schemaName: String,
                                val tableName: String,
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
    case that: RowUpdated =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fieldsNew == that.fieldsNew &&
      fieldsOld == that.fieldsOld
    case _ => false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fieldsNew, fieldsOld)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"RowUpdated(schemaName=$schemaName, tableName=$tableName, fieldsNew=$fieldsNew, fieldsOld=$fieldsOld)"
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
  def getFields: java.util.List[Field] =
    fields.asJava

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: RowDeleted =>
      schemaName == that.schemaName &&
      tableName == that.tableName &&
      fields == that.fields
    case _ => false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(schemaName, tableName, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  // auto-generated
  override def toString = s"RowDeleted(schemaName=$schemaName, tableName=$tableName, fields=$fields)"
}

object ChangeSet {

  def unapply(arg: ChangeSet): Option[(Long, String, ZonedDateTime, List[Change])] =
    Some((arg.transactionId, arg.location, arg.zonedDateTime, arg.changes))

  def apply(transactionId: Long, location: String, zonedDateTime: ZonedDateTime, changes: List[Change]): ChangeSet =
    new ChangeSet(transactionId, location, zonedDateTime, changes)

}

final class ChangeSet private (val transactionId: Long,
                               val location: String,
                               val zonedDateTime: ZonedDateTime,
                               val changes: List[Change]) {

  /**
   * Java API
   *
   * Never that this never returns an empty list
   */
  def getChanges: java.util.List[Change] =
    changes.asJava

  /**
   * Java API
   */
  def getZonedDateTime: ZonedDateTime =
    zonedDateTime

  // auto-generated
  override def equals(other: Any): Boolean = other match {
    case that: ChangeSet =>
      transactionId == that.transactionId &&
      location == that.location &&
      zonedDateTime == that.zonedDateTime &&
      changes == that.changes
    case _ => false
  }

  // auto-generated
  override def hashCode(): Int = {
    val state = Seq(transactionId, location, zonedDateTime, changes)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  // auto-generated
  override def toString =
    s"ChangeSet(transactionId=$transactionId, location=$location, zonedDateTime=$zonedDateTime, changes=$changes)"
}
