/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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

import java.util.concurrent.atomic.AtomicInteger
import io.pravega.client.tables.TableEntry
import scala.util.Success
@InternalApi private final class PravegaTableReadFlowStageLogic[K, V](
    val shape: FlowShape[K, Option[V]],
    val scope: String,
    tableName: String,
    tableSettings: TableSettings[K, V],
    familyExtractor: K => String
) extends GraphStageLogic(shape)
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _
  private var table: KeyValueTable[K, V] = _

  private val onAir = new AtomicInteger

  @volatile
  private var upstreamEnded = false;

  private val asyncPushback: AsyncCallback[(Try[TableEntry[K, V]])] = getAsyncCallback { p =>
    p match {
      case Failure(exception) =>
        log.error(exception, s"Failed to send message {}")
      case Success(kv) =>
        if (kv != null)
          push(out, Some(kv.getValue()))
        else
          push(out, None)

    }

    if (onAir.decrementAndGet == 0 && upstreamEnded) {
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
        .forKeyValueTable(tableName, tableSettings.keySerializer, tableSettings.valueSerializer, kvtClientConfig)
      log.debug("Open table {}", tableName)

    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[TableEntry[K, V]]): Unit =
    completableFuture.toScala.onComplete { t =>
      asyncPushback.invokeWithFeedback((t))
    }

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val msg = grab(in)
        onAir.incrementAndGet()
        handleSentEvent(table.get(familyExtractor(msg), msg))
      }
      override def onUpstreamFinish(): Unit = {
        log.debug("Upstream finished")
        if (onAir.get == 0)
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
    tableSettings: TableSettings[K, V],
    familyExtractor: K => String
) extends GraphStage[FlowShape[K, Option[V]]] {

  val in: Inlet[K] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[Option[V]] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[K, Option[V]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaTableReadFlowStageLogic[K, V](shape, scope, streamName, tableSettings, familyExtractor)

}
