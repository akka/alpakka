/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.pravega.impl

import java.util.concurrent.{CompletableFuture, Semaphore}
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import io.pravega.client.stream.EventStreamWriter

import scala.util.control.NonFatal
import scala.jdk.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.alpakka.pravega.WriterSettings

import scala.util.{Failure, Success, Try}
@InternalApi private final class PravegaFlowStageLogic[A](val shape: FlowShape[A, A],
                                                          val scope: String,
                                                          streamName: String,
                                                          writerSettings: WriterSettings[A])
    extends GraphStageLogic(shape)
    with PravegaWriter
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  val clientConfig = writerSettings.clientConfig

  private var writer: EventStreamWriter[A] = _

  private val semaphore = new Semaphore(writerSettings.maximumInflightMessages)

  private val asyncPushback: AsyncCallback[(Try[Void], A)] = getAsyncCallback {
    case (Failure(exception), msg) =>
      log.error(s"Failed to send message: $msg", exception)
      semaphore.release()
    case (_, msg) =>
      push(out, msg)
      semaphore.release()

  }

  /**
   * Initialization logic
   */
  override def preStart(): Unit =
    try {
      writer = createWriter(streamName, writerSettings)
    } catch {
      case NonFatal(ex) => failStage(ex)
    }

  def handleSentEvent(completableFuture: CompletableFuture[Void], msg: A): Unit =
    completableFuture.asScala.onComplete { t =>
      semaphore.acquire()
      asyncPushback.invoke((t, msg))
    }

  writerSettings.keyExtractor match {
    case Some(keyExtractor) =>
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            handleSentEvent(writer.writeEvent(keyExtractor(msg), msg), msg)
          }
        }
      )

    case None =>
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            handleSentEvent(writer.writeEvent(msg), msg)
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
    log.debug("Stopping writer")
    Try(writer.close()) match {
      case Failure(exception) =>
        log.error(exception, "Error while closing writer to stream [{}] in scope [{}}]", streamName, scope)
      case Success(value) =>
        log.debug("Closed writer to stream [{}] in scope [{}}]", streamName, scope)
    }
    close()
  }

}
@InternalApi private[pravega] final class PravegaFlow[A](scope: String,
                                                         streamName: String,
                                                         writerSettings: WriterSettings[A])
    extends GraphStage[FlowShape[A, A]] {

  val in: Inlet[A] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[A] = Outlet(Logging.simpleName(this) + ".out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PravegaFlowStageLogic[A](shape, scope, streamName, writerSettings)

}
