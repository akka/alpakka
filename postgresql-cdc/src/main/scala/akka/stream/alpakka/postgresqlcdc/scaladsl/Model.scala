/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

sealed trait Change {
  val schemaName: String
  val tableName: String
}

case class Field(columnName: String, columnType: String, value: String)

case class RowInserted(schemaName: String, tableName: String, fields: List[Field]) extends Change

case class RowUpdated(schemaName: String, tableName: String, fields: List[Field]) extends Change

case class RowDeleted(schemaName: String, tableName: String, fields: List[Field]) extends Change

case class ChangeSet(transactionId: Long, changes: List[Change]) // TODO: add timestamp
