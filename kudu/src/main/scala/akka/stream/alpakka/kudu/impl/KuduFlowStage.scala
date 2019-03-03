/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.stage._
import org.apache.kudu.Schema
import org.apache.kudu.Type._
import org.apache.kudu.client.{KuduClient, KuduTable, PartialRow}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi
private[kudu] class KuduFlowStage[A](settings: KuduTableSettings[A], kuduClient: KuduClient)
    extends GraphStage[FlowShape[A, A]] {

  override protected def initialAttributes: Attributes =
    Attributes.name("KuduFLow").and(ActorAttributes.IODispatcher)

  private val in = Inlet[A]("messages")
  private val out = Outlet[A]("result")

  override val shape = FlowShape(in, out)

  def copyToInsertRow(insertPartialRow: PartialRow, partialRow: PartialRow, schema: Schema): Unit =
    schema.getColumns.asScala.foreach { cSch =>
      val columnName = cSch.getName
      val kuduType = cSch.getType
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
    new GraphStageLogic(shape) with StageLogging with KuduCapabilities with OutHandler with InHandler {

      override protected def logSource = classOf[KuduFlowStage[A]]

      lazy val table: KuduTable =
        getOrCreateTable(kuduClient, settings.tableName, settings.schema, settings.createTableOptions)

      val session = kuduClient.newSession()

      setHandlers(in, out, this)

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = {
        val msg = grab(in)
        val insert = table.newUpsert()
        val partialRow = insert.getRow()
        copyToInsertRow(partialRow, settings.converter(msg), table.getSchema)
        session.apply(insert)
        push(out, msg)
      }

      override def postStop(): Unit = {
        log.debug("Stage completed")
        try {
          session.close()
          log.debug("session closed")
        } catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer session close")
        }
      }
    }

}
