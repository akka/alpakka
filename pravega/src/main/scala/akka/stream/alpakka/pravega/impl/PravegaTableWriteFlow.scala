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
import akka.stream.alpakka.pravega.TableWriterSettings

import scala.util.{Failure, Try}
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.KeyValueTableFactory

import io.pravega.client.tables.KeyValueTableClientConfiguration

import io.pravega.client.tables.Version
import java.util.concurrent.atomic.AtomicInteger

@InternalApi private final class PravegaTableWriteFlowStageLogic[A, K, V](
    val shape: FlowShape[A, A],
    val scope: String,
    tableName: String,
    tableWriterSettings: TableWriterSettings[K, V],
    keyValueExtractor: A => (K, V),
    familyExtractor: Option[A => String]
) extends GraphStageLogic(shape)
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _
  private var table: KeyValueTable[K, V] = _

  private val onAir = new AtomicInteger

  @volatile
  private var upstreamEnded = false;

  private val asyncPushback: AsyncCallback[(Try[Version], A)] = getAsyncCallback { p =>
    p match {
      case (Failure(exception), msg) =>
        log.error(exception, s"Failed to send message {}", msg)
      case (_, msg) =>
        push(out, msg)
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
        .withScope(scope, tableWriterSettings.clientConfig)

      table = keyValueTableFactory
        .forKeyValueTable(tableName,
                          tableWriterSettings.keySerializer,
                          tableWriterSettings.valueSerializer,
                          kvtClientConfig)
      log.debug("Open table {}", tableName)

    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[Version], msg: A): Unit =
    completableFuture.toScala.onComplete { t =>
      asyncPushback.invokeWithFeedback((t, msg))
    }

  familyExtractor match {
    case Some(familyExtractor) =>
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            val (key, value) = keyValueExtractor(msg)
            onAir.incrementAndGet()
            handleSentEvent(table.put(familyExtractor(msg), key, value), msg)
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

    case None =>
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            val (key, value) = keyValueExtractor(msg)
            onAir.incrementAndGet()

            handleSentEvent(
              table.put(null, key, value),
              msg
            )
          }
          override def onUpstreamFinish(): Unit = {
            log.debug("Upstream finished")
            completeStage()
            if (onAir.get == 0)
              completeStage()
            upstreamEnded = true
          }

        }
      )

  }

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
@InternalApi private[pravega] final class PravegaTableWriteFlow[A, K, V](
    scope: String,
    streamName: String,
    tableWriterSettings: TableWriterSettings[K, V],
    keyValueExtractor: A => (K, V),
    familyExtractor: Option[A => String]
) extends GraphStage[FlowShape[A, A]] {
  def this(scope: String,
           streamName: String,
           tableWriterSettings: TableWriterSettings[K, V],
           keyValueExtractor: A => (K, V)) = this(scope, streamName, tableWriterSettings, keyValueExtractor, None)

  def this(scope: String,
           streamName: String,
           tableWriterSettings: TableWriterSettings[K, V],
           keyValueExtractor: A => (K, V),
           familyExtractor: A => String) =
    this(scope, streamName, tableWriterSettings, keyValueExtractor, Some(familyExtractor))

  val in: Inlet[A] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[A] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaTableWriteFlowStageLogic[A, K, V](shape,
                                                 scope,
                                                 streamName,
                                                 tableWriterSettings,
                                                 keyValueExtractor,
                                                 familyExtractor)

}
