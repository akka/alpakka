/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.alpakka.pravega.{PravegaEvent, PravegaReaderGroup, ReaderSettings}
import io.pravega.client.ClientConfig

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.DurationLong
import io.pravega.client.stream.{EventStreamReader, ReaderGroup}

import scala.util.control.NonFatal
import akka.stream.ActorAttributes
import akka.stream.stage.AsyncCallback

import java.util.UUID
import scala.util.{Failure, Success, Try}

@InternalApi private final class PravegaSourcesStageLogic[A](
    shape: SourceShape[PravegaEvent[A]],
    pravegaReaderGroup: PravegaReaderGroup,
    val readerSettings: ReaderSettings[A],
    startupPromise: Promise[Done]
) extends GraphStageLogic(shape)
    with PravegaCapabilities
    with StageLogging {

  protected val scope = pravegaReaderGroup.getScope

  override protected def logSource = classOf[PravegaSourcesStageLogic[A]]

  private def out = shape.out

  private var reader: EventStreamReader[A] = _

  protected val clientConfig: ClientConfig = readerSettings.clientConfig

  private val asyncOnPull: AsyncCallback[OutHandler] = getAsyncCallback { out =>
    out.onPull()
  }

  setHandler(
    out,
    new OutHandler {

      override def onPull(): Unit = {
        val eventRead = reader.readNextEvent(readerSettings.timeout)
        if (eventRead.isCheckpoint) {
          log.debug("Checkpoint: {}", eventRead.getCheckpointName)
          onPull()
        } else {
          val event = eventRead.getEvent
          if (event == null) {
            log.debug("a timeout occurred while waiting for new messages")
            materializer.scheduleOnce(readerSettings.timeout.millis, () => asyncOnPull.invoke(this))
          } else
            push(out, new PravegaEvent(event, eventRead.getPosition, eventRead.getEventPointer))
        }
      }

    }
  )

  override def preStart(): Unit = {
    log.debug("Start consuming {}...", pravegaReaderGroup.toString)
    try {
      reader = createReader(readerSettings, pravegaReaderGroup.readerGroup)
      startupPromise.success(Done)
    } catch {
      case NonFatal(exception) =>
        log.error(exception.getMessage())
        failStage(exception)
    }
  }

  private def createReader(settings: ReaderSettings[A], readerGroup: ReaderGroup): EventStreamReader[A] =
    eventStreamClientFactory.createReader(
      settings.readerId.getOrElse(UUID.randomUUID().toString),
      readerGroup.getGroupName,
      settings.serializer,
      settings.readerConfig
    )

  override def postStop(): Unit = {
    log.debug("Stopping reader")
    Try(reader.close()) match {
      case Failure(exception) =>
        log.error(exception, s"Error while closing [{}]/[{}]", scope, pravegaReaderGroup.steamsName.mkString(", "))
      case Success(value) =>
        log.warning("Closed [{}]/[{}]", scope, pravegaReaderGroup.steamsName.mkString(", "))
    }
    close()
  }

}

@InternalApi private[pravega] final class PravegaSource[A](
    readerGroup: PravegaReaderGroup,
    settings: ReaderSettings[A]
) extends GraphStageWithMaterializedValue[SourceShape[PravegaEvent[A]], Future[Done]] {

  private val out: Outlet[PravegaEvent[A]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: SourceShape[PravegaEvent[A]] = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val startupPromise = Promise[Done]()

    val logic = new PravegaSourcesStageLogic[A](
      shape,
      readerGroup,
      settings,
      startupPromise
    )

    (logic, startupPromise.future)

  }

}
