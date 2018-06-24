/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.PostgreSQLSourceStage
import akka.stream.alpakka.postgresqlcdc._
import akka.stream.alpakka.postgresqlcdc.scaladsl.Plugins
import akka.stream.scaladsl.Source

import scala.concurrent.duration._
import scala.collection.JavaConverters._

object ChangeDataCapture {

  /**
   * Converts the events emitted (Scala Case Classes) to Java POJOs. May imply some GC pressure but it makes the API
   * so much more usable from a Java perspective.
   */
  def from(instance: javadsl.PostgreSQLInstance): akka.stream.javadsl.Source[ChangeSet, NotUsed] =
    Source
      .fromGraph(new PostgreSQLSourceStage(toScalaPostgreSQLInstance(instance)))
      .map { t: scaladsl.ChangeSet =>
        new javadsl.ChangeSet(
          t.transactionId,
          t.changes.map { change: scaladsl.Change =>
            change match {

              case scaladsl.RowInserted(schemaName, tableName, fields) =>
                new javadsl.RowInserted(schemaName, tableName, fields.map { f: scaladsl.Field =>
                  new javadsl.Field(f.columnName, f.columnType, f.value)
                }.asJava)

              case scaladsl.RowUpdated(schemaName, tableName, fields) =>
                new javadsl.RowUpdated(schemaName, tableName, fields.map { f: scaladsl.Field =>
                  new javadsl.Field(f.columnName, f.columnType, f.value)
                }.asJava)

              case scaladsl.RowDeleted(schemaName, tableName, fields) =>
                new javadsl.RowDeleted(schemaName, tableName, fields.map { f: scaladsl.Field =>
                  new javadsl.Field(f.columnName, f.columnType, f.value)
                }.asJava)
            }
          }.asJava
        )
      }
      .asJava

  private def toScalaPostgreSQLInstance(instance: javadsl.PostgreSQLInstance): scaladsl.PostgreSQLInstance =
    scaladsl.PostgreSQLInstance(
      instance.getConnectionString,
      instance.getSlotName,
      instance.getPlugin match {
        case Plugin.TestDecoding => Plugins.TestDecoding
        case Plugin.Wal2Json => Plugins.Wal2Json
      },
      instance.getMaxItems,
      instance.getDurationMillis.milliseconds
    )

}
