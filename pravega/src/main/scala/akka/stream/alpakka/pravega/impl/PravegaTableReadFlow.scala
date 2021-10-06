/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import java.util.concurrent.CompletableFuture

import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.util.control.NonFatal
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.alpakka.pravega.TableSettings

import scala.util.{Failure, Try}
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.KeyValueTableFactory

import io.pravega.client.tables.KeyValueTableClientConfiguration

import io.pravega.client.tables.TableEntry
import scala.util.Success

@InternalApi private final class PravegaTableReadFlowStageLogic[K, V](
    val shape: FlowShape[K, Option[V]],
    val scope: String,
    tableName: String,
    tableSettings: TableSettings[K, V]
) extends GraphStageLogic(shape)
    with StageLogging {

  private def in: Inlet[K] = shape.in
  private def out: Outlet[Option[V]] = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _
  private var table: KeyValueTable = _

  @volatile
  private var inFlight = 0

  @volatile
  private var upstreamEnded = false;

  private val asyncMessageSendCallback: AsyncCallback[(Try[TableEntry])] = getAsyncCallback { p =>
    p match {
      case Failure(exception) =>
        log.error(exception, s"Failed to send message {}")
      case Success(kv) =>
        if (kv != null)
          push(out, Some(tableSettings.valueSerializer.deserialize(kv.getValue())))
        else
          push(out, None)

    }
    inFlight -= 1
    if (inFlight == 0 && upstreamEnded) {
      log.info("Stage completed after upstream finish")
      completeStage()

    }
  }

  /**
   * Initialization logic
   */
  override def preStart(): Unit =
    try {
      val kvtClientConfig = KeyValueTableClientConfiguration.builder().build()

      keyValueTableFactory = KeyValueTableFactory
        .withScope(scope, tableSettings.clientConfig)

      table = keyValueTableFactory
        .forKeyValueTable(tableName, kvtClientConfig)
      log.debug("Open table {}", tableName)

    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[TableEntry]): Unit =
    completableFuture.toScala.onComplete { t =>
      asyncMessageSendCallback.invokeWithFeedback((t))
    }

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val msg = grab(in)
        inFlight += 1
        handleSentEvent(table.get(tableSettings.tableKey(msg)))
      }
      override def onUpstreamFinish(): Unit = {
        log.debug("Upstream finished")
        if (inFlight == 0)
          completeStage()
        upstreamEnded = true
      }

    }
  )

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    }
  )

  /**
   * Cleanup logic
   */
  override def postStop(): Unit = {
    log.debug("Closing table {}", tableName)
    table.close()
    keyValueTableFactory.close()
  }

}
@InternalApi private[pravega] final class PravegaTableReadFlow[K, V](
    scope: String,
    streamName: String,
    tableSettings: TableSettings[K, V]
) extends GraphStage[FlowShape[K, Option[V]]] {

  val in: Inlet[K] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[Option[V]] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[K, Option[V]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaTableReadFlowStageLogic[K, V](shape, scope, streamName, tableSettings)

}
