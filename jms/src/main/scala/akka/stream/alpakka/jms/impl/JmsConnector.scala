/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.atomic.AtomicReference

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.pattern.after
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl.InternalConnectionState._
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.stage.{AsyncCallback, StageLogging, TimerGraphStageLogic}
import akka.stream.{ActorAttributes, Attributes, OverflowStrategy}
import javax.jms

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 */
@InternalApi
private[jms] trait JmsConnector[S <: JmsSession] {
  this: TimerGraphStageLogic with StageLogging =>

  import JmsConnector._

  implicit protected var ec: ExecutionContext = _

  private var jmsSessions = Seq.empty[S]

  protected def destination: Destination

  protected def jmsSettings: JmsSettings

  protected def onSessionOpened(jmsSession: S): Unit = {}

  protected val fail: AsyncCallback[Throwable] = getAsyncCallback[Throwable](publishAndFailStage)

  private val connectionFailedCB = getAsyncCallback[Throwable](connectionFailed)

  private var connectionStateQueue: SourceQueueWithComplete[InternalConnectionState] = _

  private val connectionStateSourcePromise = Promise[Source[InternalConnectionState, NotUsed]]()

  protected val connectionStateSource: Future[Source[InternalConnectionState, NotUsed]] =
    connectionStateSourcePromise.future

  private var connectionState: InternalConnectionState = JmsConnectorDisconnected

  override def preStart(): Unit = {
    // keep two elements since the time between initializing and connected can be very short.
    // always drops the old state, and keeps the most current (two) state(s) in the queue.
    val (queue, source) =
      Source
        .queue[InternalConnectionState](2, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink(1))(Keep.both)
        .run()(this.materializer)
    connectionStateQueue = queue
    connectionStateSourcePromise.complete(Success(source))

    // add subscription to purge queued connection status events after the configured timeout.
    scheduleOnce(ConnectionStatusTimeout, jmsSettings.connectionStatusSubscriptionTimeout)
  }

  protected def finishStop(): Unit = {
    val update: InternalConnectionState => InternalConnectionState = {
      case JmsConnectorStopping(completion) => JmsConnectorStopped(completion)
      case stopped: JmsConnectorStopped => stopped
      case current =>
        JmsConnectorStopped(
          Failure(new IllegalStateException(s"Completing stage stop in unexpected state ${current.getClass}"))
        )
    }

    closeSessions()
    val previous = updateStateWith(update)
    closeConnectionAsync(connection(previous))
    if (isTimerActive("connection-status-timeout")) drainConnectionState()
    connectionStateQueue.complete()
  }

  protected def publishAndFailStage(ex: Throwable): Unit = {
    val previous = updateState(JmsConnectorStopping(Failure(ex)))
    closeConnectionAsync(connection(previous))
    failStage(ex)
  }

  protected def updateState(next: InternalConnectionState): InternalConnectionState = {
    val update: InternalConnectionState => InternalConnectionState = {
      case current: JmsConnectorStopping => current
      case current: JmsConnectorStopped => current
      case _ => next
    }
    updateStateWith(update)
  }

  private def updateStateWith(f: InternalConnectionState => InternalConnectionState): InternalConnectionState = {
    val last = connectionState
    connectionState = f(last)

    // use type-based comparison to publish JmsConnectorInitializing only once.
    if (last.getClass != connectionState.getClass) {
      if (log.isDebugEnabled)
        log.debug("updateStateWith {} -> {}", last.getClass.getSimpleName, connectionState.getClass.getSimpleName)
      connectionStateQueue.offer(connectionState)
    }

    last
  }

  protected def connectionFailed(ex: Throwable): Unit = ex match {
    case ex: jms.JMSSecurityException =>
      log.error(
        ex,
        "{} initializing connection failed, security settings are not properly configured for destination[{}]",
        attributes.nameLifted.mkString,
        destination.name
      )
      publishAndFailStage(ex)

    case _: jms.JMSException | _: JmsConnectTimedOut => handleRetriableException(ex)

    case _ =>
      connectionState match {
        case _: JmsConnectorStopping | _: JmsConnectorStopped => logStoppingException(ex)
        case _ =>
          log.error(ex, "{} connection failed for destination[{}]", attributes.nameLifted.mkString, destination.name)
          publishAndFailStage(ex)
      }
  }

  private def handleRetriableException(ex: Throwable): Unit = {
    jmsSessions = Seq.empty
    connectionState match {
      case JmsConnectorInitializing(_, attempt, backoffMaxed, _) =>
        maybeReconnect(ex, attempt, backoffMaxed)
      case JmsConnectorConnected(_) | JmsConnectorDisconnected =>
        maybeReconnect(ex, 0, backoffMaxed = false)
      case _: JmsConnectorStopping | _: JmsConnectorStopped => logStoppingException(ex)
    }
  }

  private def logStoppingException(ex: Throwable): Unit =
    log.info("{} caught exception {} while stopping stage: {}",
             attributes.nameLifted.mkString,
             ex.getClass.getSimpleName,
             ex.getMessage)

  private val onSession: AsyncCallback[S] = getAsyncCallback[S] { session =>
    jmsSessions :+= session
    onSessionOpened(session)
  }

  protected val sessionOpened: Try[Unit] => Unit = {
    case Success(_) =>
      connectionState match {
        case init @ JmsConnectorInitializing(c, _, _, sessions) =>
          if (sessions + 1 == jmsSettings.sessionCount) {
            c.foreach { c =>
              updateState(JmsConnectorConnected(c))
              log.info("{} connected", attributes.nameLifted.mkString)
            }
          } else {
            updateState(init.copy(sessions = sessions + 1))
          }
        case s => ()
      }

    case Failure(ex: jms.JMSException) =>
      updateState(JmsConnectorDisconnected) match {
        case JmsConnectorInitializing(c, attempt, backoffMaxed, _) =>
          closeConnectionAsync(c)
          maybeReconnect(ex, attempt, backoffMaxed)
        case _ => ()
      }

    case Failure(ex) =>
      log.error(ex,
                "{} initializing connection failed for destination[{}]",
                attributes.nameLifted.mkString,
                destination.name)
      publishAndFailStage(ex)
  }

  protected val sessionOpenedCB: AsyncCallback[Try[Unit]] = getAsyncCallback[Try[Unit]](sessionOpened)

  private def maybeReconnect(ex: Throwable, attempt: Int, backoffMaxed: Boolean): Unit = {
    val retrySettings = jmsSettings.connectionRetrySettings
    import retrySettings._
    val nextAttempt = attempt + 1
    if (maxRetries >= 0 && nextAttempt > maxRetries) {
      val exception =
        if (maxRetries == 0) ex
        else ConnectionRetryException(s"Could not establish connection after $maxRetries retries.", ex)
      log.error(exception,
                "{} initializing connection failed for destination[{}]",
                attributes.nameLifted.mkString,
                destination.name)
      publishAndFailStage(exception)
    } else {
      val status = updateState(JmsConnectorDisconnected)
      closeConnectionAsync(connection(status))
      val delay = if (backoffMaxed) maxBackoff else waitTime(nextAttempt)
      val backoffNowMaxed = backoffMaxed || delay == maxBackoff
      scheduleOnce(AttemptConnect(nextAttempt, backoffNowMaxed), delay)
    }
  }

  override def onTimer(timerKey: Any): Unit = timerKey match {
    case FlushAcknowledgementsTimerKey(session, timeout) =>
      session.ackQueue.forEach(_.apply())
      session.ackQueue.clear()
      session.pendingAck = 0
      scheduleOnce(timerKey, timeout)

    case AttemptConnect(attempt, backoffMaxed) =>
      log.info("{} retries connecting, attempt {}", attributes.nameLifted.mkString, attempt)
      initSessionAsync(attempt, backoffMaxed)
    case ConnectionStatusTimeout => drainConnectionState()
    case _ => ()
  }

  private def drainConnectionState(): Unit =
    Source.future(connectionStateSource).flatMapConcat(identity).runWith(Sink.ignore)(this.materializer)

  protected def executionContext(attributes: Attributes): ExecutionContext = {
    val dispatcherId = (attributes.get[ActorAttributes.Dispatcher](ActorAttributes.IODispatcher) match {
      case ActorAttributes.Dispatcher("") =>
        ActorAttributes.IODispatcher
      case d => d
    }) match {
      case d @ ActorAttributes.IODispatcher =>
        // this one is not a dispatcher id, but is a config path pointing to the dispatcher id
        materializer.system.settings.config.getString(d.dispatcher)
      case d => d.dispatcher
    }

    materializer.system.dispatchers.lookup(dispatcherId)
  }

  protected def createSession(connection: jms.Connection, createDestination: jms.Session => jms.Destination): S

  protected def initSessionAsync(attempt: Int = 0, backoffMaxed: Boolean = false): Unit = {
    val allSessions = openSessions(attempt, backoffMaxed)
    allSessions.failed.foreach(connectionFailedCB.invoke)(ExecutionContexts.parasitic)
    // wait for all sessions to successfully initialize before invoking the onSession callback.
    // reduces flakiness (start, consume, then crash) at the cost of increased latency of startup.
    allSessions.foreach(_.foreach(onSession.invoke))
  }

  protected def closeConnection(connection: jms.Connection): Unit = {
    try {
      // deregister exception listener to clear reference from JMS client to the Akka stage
      connection.setExceptionListener(null)
    } catch {
      case _: jms.JMSException => // ignore
    }
    try {
      connection.close()
      log.debug("JMS connection {} closed", connection)
    } catch {
      case NonFatal(e) => log.warning("Error closing JMS connection {}: {}", connection, e)
    }
  }

  protected def closeConnectionAsync(eventualConnection: Future[jms.Connection]): Future[Done] =
    eventualConnection.map(closeConnection).map(_ => Done)

  protected def closeSessions(): Unit = {
    jmsSessions.foreach(s => closeSession(s))
    jmsSessions = Seq.empty
  }

  protected def closeSessionsAsync(): Future[Unit] = {
    val closing = Future
      .sequence {
        jmsSessions.map(s => Future(closeSession(s)))
      }
      .map(_ => ())
    jmsSessions = Seq.empty
    closing
  }

  private def closeSession(s: S): Unit = {
    try s.closeSession()
    catch {
      case e: Throwable => log.error(e, "Error closing jms session")
    }
  }

  protected def abortSessionsAsync(): Future[Unit] = {
    val aborting = Future
      .sequence {
        jmsSessions.map { s =>
          Future {
            try s.abortSession()
            catch {
              case e: Throwable => log.error(e, "Error aborting jms session")
            }
          }
        }
      }
      .map(_ => ())
    jmsSessions = Seq.empty
    aborting
  }

  def startConnection: Boolean

  private def openSessions(attempt: Int, backoffMaxed: Boolean): Future[Seq[S]] = {
    val eventualConnection = openConnection(attempt, backoffMaxed)
    eventualConnection.flatMap { connection =>
      val sessionFutures =
        for (_ <- 0 until jmsSettings.sessionCount)
          yield Future(createSession(connection, destination.create))
      Future.sequence(sessionFutures)
    }(ExecutionContexts.parasitic)
  }

  private def openConnection(attempt: Int, backoffMaxed: Boolean): Future[jms.Connection] = {
    implicit val system: ActorSystem = materializer.system
    val jmsConnection = openConnectionAttempt(startConnection)
    updateState(JmsConnectorInitializing(jmsConnection, attempt, backoffMaxed, 0))
    jmsConnection.map { connection =>
      connection.setExceptionListener(new jms.ExceptionListener {
        override def onException(ex: jms.JMSException): Unit = {
          closeConnection(connection)
          connectionFailedCB.invoke(ex)
        }
      })
      connection
    }
  }

  private def openConnectionAttempt(startConnection: Boolean)(implicit system: ActorSystem): Future[jms.Connection] = {
    val factory = jmsSettings.connectionFactory
    val connectionRef = new AtomicReference[Option[jms.Connection]](None)

    // status is also the decision point between the two futures below which one will win.
    val status = new AtomicReference[ConnectionAttemptStatus](Connecting)

    val connectionFuture = Future {
      val connection = jmsSettings.credentials match {
        case Some(c: Credentials) => factory.createConnection(c.username, c.password)
        case _ => factory.createConnection()
      }
      if (status.get == Connecting) { // `TimedOut` can be set at any point. So we have to check whether to continue.
        connectionRef.set(Some(connection))
        if (startConnection) connection.start()
      }
      // ... and close if the connection is not to be used, don't return the connection
      if (!status.compareAndSet(Connecting, Connected)) {
        connectionRef.get.foreach(closeConnection)
        connectionRef.set(None)
        throw JmsConnectTimedOut("Received timed out signal trying to establish connection")
      } else connection
    }

    val connectTimeout = jmsSettings.connectionRetrySettings.connectTimeout
    val timeoutFuture = after(connectTimeout, system.scheduler) {
      // Even if the timer goes off, the connection may already be good. We use the
      // status field and an atomic compareAndSet to see whether we should indeed time out, or just return
      // the connection. In this case it does not matter which future returns. Both will have the right answer.
      if (status.compareAndSet(Connecting, TimedOut)) {
        connectionRef.get.foreach(closeConnection)
        connectionRef.set(None)
        Future.failed(
          JmsConnectTimedOut(
            s"Timed out after $connectTimeout trying to establish connection. " +
            "Please see ConnectionRetrySettings.connectTimeout"
          )
        )
      } else
        connectionRef.get match {
          case Some(connection) => Future.successful(connection)
          case None => Future.failed(new IllegalStateException("BUG: Connection reference not set when connected"))
        }
    }

    Future.firstCompletedOf(Iterator(connectionFuture, timeoutFuture))(ExecutionContexts.parasitic)
  }
}

/**
 * Internal API.
 */
@InternalApi
object JmsConnector {

  sealed trait ConnectionAttemptStatus
  case object Connecting extends ConnectionAttemptStatus
  case object Connected extends ConnectionAttemptStatus
  case object TimedOut extends ConnectionAttemptStatus

  case class AttemptConnect(attempt: Int, backoffMaxed: Boolean)
  case class FlushAcknowledgementsTimerKey(jmsSession: JmsAckSession, timeout: FiniteDuration)
  case object ConnectionStatusTimeout

  def connection: InternalConnectionState => Future[jms.Connection] = {
    case InternalConnectionState.JmsConnectorInitializing(c, _, _, _) => c
    case InternalConnectionState.JmsConnectorConnected(c) => Future.successful(c)
    case _ => Future.failed(JmsNotConnected)
  }
}
