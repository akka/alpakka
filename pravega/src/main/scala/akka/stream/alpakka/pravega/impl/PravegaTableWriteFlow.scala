/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.pravega.impl

import java.util.concurrent.CompletableFuture
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.util.control.NonFatal
import scala.jdk.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.alpakka.pravega.TableWriterSettings

import scala.util.{Failure, Success, Try}
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration
import io.pravega.client.tables.Version

import java.util.concurrent.atomic.AtomicInteger

import io.pravega.client.tables.Put

import io.pravega.client.tables.TableKey

@InternalApi private final class PravegaTableWriteFlowStageLogic[KVPair, K, V](
    val shape: FlowShape[KVPair, KVPair],
    kvpToTuple2: KVPair => (K, V),
    extractor: K => TableKey,
    val scope: String,
    tableName: String,
    tableWriterSettings: TableWriterSettings[K, V]
) extends GraphStageLogic(shape)
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _

  private var table: KeyValueTable = _

  private val onAir = new AtomicInteger

  @volatile
  private var upstreamEnded = false

  private val asyncPushback: AsyncCallback[(Try[Version], KVPair)] = getAsyncCallback { p =>
    p match {
      case (Failure(exception), msg) =>
        log.error(exception, s"Failed to send message {}", msg)
      case (_, msg) =>
        push(out, msg)
    }

    if (onAir.decrementAndGet == 0 && upstreamEnded) {
      log.debug("Stage completed after upstream finish")
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
        .withScope(scope, tableWriterSettings.clientConfig)

      table = keyValueTableFactory
        .forKeyValueTable(tableName, kvtClientConfig)
      log.debug("Open table {}", tableName)

    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[Version], msg: KVPair): Unit =
    completableFuture.asScala.onComplete { t =>
      asyncPushback.invokeWithFeedback((t, msg))
    }

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val msg = grab(in)

        val (k, v) = kvpToTuple2(msg)

        val put = new Put(
          extractor(k),
          tableWriterSettings.valueSerializer.serialize(v)
        )
        onAir.incrementAndGet()
        handleSentEvent(table.update(put), msg)
      }
      override def onUpstreamFinish(): Unit = {
        log.debug("Upstream finished")
        if (onAir.get == 0) {
          log.debug("Stage completed on upstream finish")
          completeStage()
        }
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
    Try(table.close()) match {
      case Failure(exception) =>
        log.error(exception, "Error while closing table [{}]", tableName)
      case Success(value) =>
        log.debug("Closed table [{}]", tableName)
    }
    keyValueTableFactory.close()
  }

}
@InternalApi private[pravega] final class PravegaTableWriteFlow[KVPair, K, V](
    kvpToTuple2: KVPair => Tuple2[K, V],
    scope: String,
    streamName: String,
    tableWriterSettings: TableWriterSettings[K, V]
) extends GraphStage[FlowShape[KVPair, KVPair]] {

  val in: Inlet[KVPair] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[KVPair] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[KVPair, KVPair] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaTableWriteFlowStageLogic[KVPair, K, V](shape,
                                                      kvpToTuple2,
                                                      tableWriterSettings.tableKey,
                                                      scope,
                                                      streamName,
                                                      tableWriterSettings)

}
