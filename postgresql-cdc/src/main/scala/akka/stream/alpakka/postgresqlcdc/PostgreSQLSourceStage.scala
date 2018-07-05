/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.{Connection, PreparedStatement}

import akka.NotUsed
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}

import scala.collection.mutable
import scala.util.control.NonFatal

@InternalApi
private[postgresqlcdc] final class PostgreSQLSourceStage(instance: PostgreSQLInstance,
                                                         settings: ChangeDataCaptureSettings)
    extends GraphStage[SourceShape[ChangeSet]] {

  private val out: Outlet[ChangeSet] = Outlet[ChangeSet]("postgresqlcdc.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PostgreSQLSourceStageLogic(instance, settings, shape)

  override def shape: SourceShape[ChangeSet] = SourceShape(out)

}

@InternalApi
private[postgresqlcdc] final class PostgreSQLSourceStageLogic(val instance: PostgreSQLInstance,
                                                              val settings: ChangeDataCaptureSettings,
                                                              val shape: SourceShape[ChangeSet])
    extends TimerGraphStageLogic(shape)
    with StageLogging {

  import PostgreSQL._

  private lazy val getSlotChangesStatement: PreparedStatement =
    buildGetSlotChangesStmt(slotName = instance.slotName, maxItems = settings.maxItems)
  private val buffer = new mutable.Queue[ChangeSet]()

  private implicit lazy val conn: Connection = getConnection(instance.jdbcConnectionString)

  private implicit lazy val logging: LoggingAdapter = log // bring log into implicit scope

  override def onTimer(timerKey: Any): Unit =
    retrieveChanges()

  private def retrieveChanges(): Unit = {

    val result: List[ChangeSet] = {
      val slotChanges = getSlotChanges(getSlotChangesStatement)
      TestDecodingPlugin.transformSlotChanges(slotChanges, settings.tablesToIgnore, settings.columnsToIgnore)
    }

    if (result.nonEmpty) {
      buffer ++= result
      push(out, buffer.dequeue())
    } else if (isAvailable(out))
      scheduleOnce(NotUsed, settings.pollInterval)
  }

  private def out = shape.out

  override def preStart(): Unit = {
    val slotExists = checkSlotExists(instance.slotName)
    if (!slotExists && settings.createSlotOnStart)
      setUpSlot(instance.slotName)
  }

  override def postStop(): Unit =
    try {
      conn.close()
      log.debug("closed connection")
    } catch {
      case NonFatal(e) =>
        log.error("failed to close connection", e)
    }

  setHandler(out, new OutHandler {

    override def onPull(): Unit =
      if (buffer.nonEmpty)
        push(out, buffer.dequeue())
      else
        retrieveChanges()
  })

}
