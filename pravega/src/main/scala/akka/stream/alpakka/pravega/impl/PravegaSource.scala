/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.alpakka.pravega.{PravegaEvent, ReaderSettings}
import io.pravega.client.ClientConfig

import scala.concurrent.{Future, Promise}
import io.pravega.client.stream.{EventStreamReader, ReaderGroup}

import scala.util.control.NonFatal
import akka.stream.ActorAttributes
@InternalApi private final class PravegaSourcesStageLogic[A](
    shape: SourceShape[PravegaEvent[A]],
    val scope: String,
    streamName: String,
    val readerSettings: ReaderSettings[A],
    startupPromise: Promise[Done]
) extends GraphStageLogic(shape)
    with PravegaReader
    with StageLogging {

  override protected def logSource = classOf[PravegaSourcesStageLogic[A]]

  private def out = shape.out

  private var reader: Reader[A] = _

  val clientConfig: ClientConfig = readerSettings.clientConfig

  setHandler(
    out,
    new OutHandler {

      override def onPull(): Unit = {
        val eventRead = reader.nextEvent(readerSettings.timeout)
        if (eventRead.isCheckpoint) {
          log.debug("Checkpoint: {}", eventRead.getCheckpointName)
          onPull()
        } else {
          val event = eventRead.getEvent
          if (event == null) {
            log.debug("a timeout occurred while waiting for new messages")
          } else
            push(out, new PravegaEvent(event, eventRead.getPosition, eventRead.getEventPointer))
        }
      }
    }
  )

  override def preStart(): Unit = {
    log.debug("Start consuming {}...", streamName)
    try {
      reader = createReader(readerSettings, streamName)
      startupPromise.success(Done)
    } catch {
      case NonFatal(exception) =>
        log.error(exception.getMessage())
        failStage(exception)
    }
  }

  override def postStop(): Unit = {
    log.debug("Stopping reader")
    reader.close()
    close()
  }

}

@InternalApi private[pravega] final class PravegaSource[A](
    scope: String,
    streamName: String,
    settings: ReaderSettings[A]
) extends GraphStageWithMaterializedValue[SourceShape[PravegaEvent[A]], Future[Done]] {

  private val out: Outlet[PravegaEvent[A]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: SourceShape[PravegaEvent[A]] = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val startupPromise = Promise[Done]

    val logic = new PravegaSourcesStageLogic[A](
      shape,
      scope,
      streamName,
      settings,
      startupPromise
    )

    (logic, startupPromise.future)

  }

}

@InternalApi private[pravega] class Reader[A](readerGroup: ReaderGroup, eventStreamReader: EventStreamReader[A]) {

  def nextEvent(timeout: Long) = eventStreamReader.readNextEvent(timeout)

  def close(): Unit = {
    eventStreamReader.close()
    readerGroup.close()
  }

}
