/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.sql.Connection

import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.stream._
import akka.stream.stage._

import scala.util.control.NonFatal

@InternalApi
private[postgresqlcdc] final class PostgreSQLAckSinkStage(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings)
    extends GraphStage[SinkShape[ChangeSet]] {

  private val in: Inlet[ChangeSet] = Inlet[ChangeSet]("postgresqlcdc.in")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PostgreSQLSinkStageLogic(instance, settings, shape)

  override def shape: SinkShape[ChangeSet] = SinkShape(in)
}
@InternalApi
private[postgresqlcdc] final class PostgreSQLSinkStageLogic(val instance: PostgreSQLInstance,
                                                            val settings: PgCdcAckSinkSettings,
                                                            val shape: SinkShape[ChangeSet])
    extends TimerGraphStageLogic(shape)
    with StageLogging {

  import PostgreSQL._

  private implicit lazy val conn: Connection = getConnection(instance.jdbcConnectionString)

  private implicit lazy val logging: LoggingAdapter = log // bring log into implicit scope

  private def in: Inlet[ChangeSet] = shape.in

  override def onTimer(timerKey: Any): Unit = {}

  override def preStart(): Unit =
    pull(shape.in)

  setHandler(in, new InHandler {
    override def onPush(): Unit = {
      val e: ChangeSet = grab(in)
      pull(in)
    }
  })

  override def postStop(): Unit =
    try {
      conn.close()
      log.debug("closed connection")
    } catch {
      case NonFatal(e) =>
        log.error("failed to close connection", e)
    }

}
