/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.Connection

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage._
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import scala.util.control.NonFatal

@InternalApi
private[postgresqlcdc] final class PostgreSQLAckSinkStage(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings)
    extends GraphStage[SinkShape[AckLogSeqNum]] {

  override def initialAttributes: Attributes = super.initialAttributes and IODispatcher

  private val in: Inlet[AckLogSeqNum] = Inlet[AckLogSeqNum]("postgresqlcdc.in")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PostgreSQLSinkStageLogic(instance, settings, shape)

  override def shape: SinkShape[AckLogSeqNum] = SinkShape(in)
}
@InternalApi
private[postgresqlcdc] final class PostgreSQLSinkStageLogic(val instance: PostgreSQLInstance,
                                                            val settings: PgCdcAckSinkSettings,
                                                            val shape: SinkShape[AckLogSeqNum])
    extends TimerGraphStageLogic(shape)
    with StageLogging {

  import PostgreSQL._

  private var items: List[String] = List.empty[String] // LSNs of un-acked items (cannot grow > settings.maxItems)

  private implicit lazy val conn: Connection = getConnection(instance.jdbcConnectionString)

  private def in: Inlet[AckLogSeqNum] = shape.in

  override def onTimer(timerKey: Any): Unit = {
    acknowledgeItems()
    scheduleOnce("postgresqlcdc-ack-sink-timer", settings.maxItemsWait)
  }

  private def acknowledgeItems(): Unit =
    items.headOption match {
      case Some(v) ⇒
        flush(instance.slotName, v)
        items = Nil
      case None ⇒
        log.debug("no items to acknowledge consumption of")
    }

  override def preStart(): Unit =
    pull(shape.in)

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val e: AckLogSeqNum = grab(in)
        items = e.logSeqNum :: items
        if (items.size == settings.maxItems)
          acknowledgeItems()
        pull(in)
      }
    }
  )

  override def postStop(): Unit =
    try {
      conn.close()
      log.debug("closed connection")
    } catch {
      case NonFatal(e) ⇒
        log.error("failed to close connection", e)
    }

}
