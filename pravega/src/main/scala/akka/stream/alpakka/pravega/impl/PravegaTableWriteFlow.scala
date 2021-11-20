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
import akka.stream.alpakka.pravega.TableWriterSettings

import scala.util.{Failure, Success, Try}
import io.pravega.client.tables.KeyValueTable
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.KeyValueTableClientConfiguration
import io.pravega.client.tables.Version

import java.util.concurrent.atomic.AtomicInteger
@InternalApi private final class PravegaTableWriteFlowStageLogic[KVPair, K, V](
    val shape: FlowShape[KVPair, KVPair],
    kvpToTuple2: KVPair => (K, V),
    val scope: String,
    tableName: String,
    tableWriterSettings: TableWriterSettings[K, V],
    keyFamilyExtractor: Option[K => String]
) extends GraphStageLogic(shape)
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _

  private var table: KeyValueTable[K, V] = _

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
        .forKeyValueTable(tableName,
                          tableWriterSettings.keySerializer,
                          tableWriterSettings.valueSerializer,
                          kvtClientConfig)
      log.debug("Open table {}", tableName)

    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[Version], msg: KVPair): Unit =
    completableFuture.toScala.onComplete { t =>
      asyncPushback.invokeWithFeedback((t, msg))
    }

  keyFamilyExtractor match {
    case Some(familyExtractor) =>
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            val (key, value) = kvpToTuple2(msg)
            onAir.incrementAndGet()
            handleSentEvent(table.put(familyExtractor(key), key, value), msg)
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
            val (key, value) = kvpToTuple2(msg)
            onAir.incrementAndGet()
            handleSentEvent(table.put(null, key, value), msg)
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
    tableWriterSettings: TableWriterSettings[K, V],
    keyFamilyExtractor: Option[K => String]
) extends GraphStage[FlowShape[KVPair, KVPair]] {

  def this(kvpToTuple2: KVPair => Tuple2[K, V],
           scope: String,
           streamName: String,
           tableWriterSettings: TableWriterSettings[K, V]) =
    this(kvpToTuple2, scope, streamName, tableWriterSettings, None)

  def this(kvpToTuple2: KVPair => Tuple2[K, V],
           scope: String,
           streamName: String,
           tableWriterSettings: TableWriterSettings[K, V],
           familyExtractor: K => String) =
    this(kvpToTuple2, scope, streamName, tableWriterSettings, Some(familyExtractor))

  val in: Inlet[KVPair] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[KVPair] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[KVPair, KVPair] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaTableWriteFlowStageLogic[KVPair, K, V](shape,
                                                      kvpToTuple2,
                                                      scope,
                                                      streamName,
                                                      tableWriterSettings,
                                                      keyFamilyExtractor)

}
