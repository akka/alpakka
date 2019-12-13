/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.atomic.AtomicBoolean

import akka.annotation.InternalApi
import akka.stream.alpakka.jms.impl.InternalConnectionState.JmsConnectorStopping
import akka.stream.alpakka.jms.{Destination, JmsConsumerSettings}
import akka.stream.scaladsl.Source
import akka.stream.stage.{OutHandler, StageLogging, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.{Done, NotUsed}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import javax.jms

/**
 * Internal API.
 */
@InternalApi
private trait JmsConsumerConnector extends JmsConnector[JmsConsumerSession] {
  this: TimerGraphStageLogic with StageLogging =>

  override val startConnection = true

  protected def createSession(connection: jms.Connection,
                              createDestination: jms.Session => jms.Destination): JmsConsumerSession

}

/**
 * Internal API.
 */
@InternalApi
private abstract class SourceStageLogic[T](shape: SourceShape[T],
                                           out: Outlet[T],
                                           settings: JmsConsumerSettings,
                                           val destination: Destination,
                                           inheritedAttributes: Attributes)
    extends TimerGraphStageLogic(shape)
    with JmsConsumerConnector
    with StageLogging {

  override protected def jmsSettings: JmsConsumerSettings = settings
  private val queue = mutable.Queue[T]()
  private val stopping = new AtomicBoolean(false)
  private var stopped = false

  private val markStopped = getAsyncCallback[Done.type] { _ =>
    stopped = true
    finishStop()
    if (queue.isEmpty) completeStage()
  }

  private val markAborted = getAsyncCallback[Throwable] { ex =>
    stopped = true
    finishStop()
    failStage(ex)
  }

  protected val handleError = getAsyncCallback[Throwable] { e =>
    updateState(JmsConnectorStopping(Failure(e)))
    fail(out, e)
  }

  override def preStart(): Unit = {
    super.preStart()
    ec = executionContext(inheritedAttributes)
    initSessionAsync()
  }

  private[jms] val handleMessage = getAsyncCallback[T] { msg =>
    if (isAvailable(out)) {
      if (queue.isEmpty) {
        pushMessage(msg)
      } else {
        pushMessage(queue.dequeue())
        queue.enqueue(msg)
      }
    } else {
      queue.enqueue(msg)
    }
  }

  protected def pushMessage(msg: T): Unit

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = {
        if (queue.nonEmpty) pushMessage(queue.dequeue())
        if (stopped && queue.isEmpty) completeStage()
      }

      override def onDownstreamFinish(): Unit = {
        // no need to keep messages in the queue, downstream will never pull them.
        queue.clear()
        // keep processing async callbacks for stopSessions.
        setKeepGoing(true)
        stopSessions()
      }
    }
  )

  private def stopSessions(): Unit =
    if (stopping.compareAndSet(false, true)) {
      val status = updateState(JmsConnectorStopping(Success(Done)))
      val connectionFuture = JmsConnector.connection(status)

      Future
        .sequence(closeSessionsAsync())
        .onComplete { _ =>
          connectionFuture
            .map { connection =>
              try {
                connection.close()
              } catch {
                case NonFatal(e) => log.error(e, "Error closing JMS connection {}", connection)
              }
            }
            .onComplete { _ =>
              // By this time, after stopping connection, closing sessions, all async message submissions to this
              // stage should have been invoked. We invoke markStopped as the last item so it gets delivered after
              // all JMS messages are delivered. This will allow the stage to complete after all pending messages
              // are delivered, thus preventing message loss due to premature stage completion.
              markStopped.invoke(Done)
            }
        }
    }

  private def abortSessions(ex: Throwable): Unit =
    if (stopping.compareAndSet(false, true)) {
      if (log.isDebugEnabled) log.debug("aborting sessions ({})", ex.toString)
      val status = updateState(JmsConnectorStopping(Failure(ex)))
      val connectionFuture = JmsConnector.connection(status)
      Future
        .sequence(abortSessionsAsync())
        .onComplete { _ =>
          connectionFuture
            .map { connection =>
              try {
                connection.close()
                log.info("JMS connection {} closed", connection)
              } catch {
                case NonFatal(e) => log.error(e, "Error closing JMS connection {}", connection)
              }
            }
            .onComplete { _ =>
              markAborted.invoke(ex)
            }
        }
    }

  def consumerControl: JmsConsumerMatValue = new JmsConsumerMatValue {
    override def shutdown(): Unit = stopSessions()
    override def abort(ex: Throwable): Unit = abortSessions(ex)
    override def connected: Source[InternalConnectionState, NotUsed] =
      Source.fromFuture(connectionStateSource).flatMapConcat(identity)
  }

  override def postStop(): Unit = finishStop()
}
