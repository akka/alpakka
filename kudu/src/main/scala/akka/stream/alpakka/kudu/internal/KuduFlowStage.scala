/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.internal

import akka.stream._
import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.stage._
import org.apache.kudu.Type._
import org.apache.kudu.Schema
import org.apache.kudu.client.{KuduTable, PartialRow}

import scala.util.control.NonFatal

private[kudu] class KuduFlowStage[A](settings: KuduTableSettings[A]) extends GraphStage[FlowShape[A, A]] {

  override protected def initialAttributes: Attributes =
    Attributes.name("KuduFLow").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  private val in = Inlet[A]("messages")
  private val out = Outlet[A]("result")

  override val shape = FlowShape(in, out)

  def copyToInsertRow(insertPartialRow: PartialRow, partialRow: PartialRow, schema: Schema): Unit =
    schema.getColumns.forEach { schema =>
      val columnName = schema.getName
      val kuduType = schema.getType
      kuduType match {
        case INT8 => insertPartialRow.addByte(columnName, partialRow.getByte(columnName))
        case INT16 => insertPartialRow.addShort(columnName, partialRow.getShort(columnName))
        case INT32 => insertPartialRow.addInt(columnName, partialRow.getInt(columnName))
        case INT64 => insertPartialRow.addLong(columnName, partialRow.getLong(columnName))
        case BINARY => insertPartialRow.addBinary(columnName, partialRow.getBinary(columnName))
        case STRING => insertPartialRow.addString(columnName, partialRow.getString(columnName))
        case BOOL => insertPartialRow.addBoolean(columnName, partialRow.getBoolean(columnName))
        case FLOAT => insertPartialRow.addFloat(columnName, partialRow.getFloat(columnName))
        case DOUBLE => insertPartialRow.addDouble(columnName, partialRow.getDouble(columnName))
        case _ => throw new UnsupportedOperationException(s"Unknown type ${kuduType}")
      }
    }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging with KuduCapabilities {

      override protected def logSource = classOf[KuduFlowStage[A]]

      val table: KuduTable =
        getOrCreateTable(settings.kuduClient, settings.tableName, settings.schema, settings.createTableOptions)

      val session = settings.kuduClient.newSession()

      setHandler(out, new OutHandler {
        override def onPull() =
          pull(in)
      })

      setHandler(
        in,
        new InHandler {
          override def onPush() = {
            val msg = grab(in)
            val insert = table.newInsert()
            val partialRow = insert.getRow()
            copyToInsertRow(partialRow, settings.converter(msg), table.getSchema)
            session.apply(insert)
            push(out, msg)
          }
        }
      )

      override def postStop() = {
        log.debug("Stage completed")
        try {
          session.close()
          log.debug("session closed")
        } catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer session close")
        }
        try {
          settings.kuduClient.shutdown()
          log.debug("client connection closed")
        } catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer connection close")
        }
      }
    }

}
