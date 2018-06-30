/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.util

import scala.collection.JavaConverters._

sealed abstract class Change {
  val schemaName: String
  val tableName: String
}

final case class Field(columnName: String, columnType: String, value: String)

final case class RowInserted(schemaName: String, tableName: String, fields: List[Field]) extends Change

final case class RowUpdated(schemaName: String, tableName: String, fields: List[Field]) extends Change

final case class RowDeleted(schemaName: String, tableName: String, fields: List[Field]) extends Change {

  /**
   * Java API
   */
  def getFields(): util.List[Field] =
    fields.asJava

}

final case class ChangeSet(transactionId: Long, changes: List[Change]) {

  /**
   * Java API
   */
  def getChanges(): util.List[Change] =
    changes.asJava

} // TODO: add timestamp
