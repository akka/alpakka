/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.pattern.after
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl.JmsConnector.{JmsConnectorState, _}
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import akka.stream.stage.{AsyncCallback, StageLogging, TimerGraphStageLogic}
import akka.stream.{ActorAttributes, ActorMaterializerHelper, Attributes, OverflowStrategy}
import akka.{Done, NotUsed}
import javax.jms

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 */
@InternalApi
private[jms] trait JmsConnector[S <: JmsSession] {
  this: TimerGraphStageLogic with StageLogging =>

  implicit protected var ec: ExecutionContext = _

  protected var jmsSessions = Seq.empty[S]

  protected def destination: Destination

  protected def jmsSettings: JmsSettings

  protected def onSessionOpened(jmsSession: S): Unit = {}

  protected val fail: AsyncCallback[Throwable] = getAsyncCallback[Throwable](publishAndFailStage)

  private val connectionFailedCB = getAsyncCallback[Throwable](connectionFailed)

  private var connectionStateQueue: SourceQueueWithComplete[JmsConnectorState] = _

  private val connectionStateSourcePromise = Promise[Source[JmsConnectorState, NotUsed]]()

  protected val connectionStateSource: Future[Source[JmsConnectorState, NotUsed]] =
    connectionStateSourcePromise.future

  private var connectionState: JmsConnectorState = JmsConnectorDisconnected

  override def preStart(): Unit = {
    // keep two elements since the time between initializing and connected can be very short.
    // always drops the old state, and keeps the most current (two) state(s) in the queue.
    val pair = Source.queue[JmsConnectorState](2, OverflowStrategy.dropHead).preMaterialize()(this.materializer)
    connectionStateQueue = pair._1
    connectionStateSourcePromise.complete(Success(pair._2))
  }

  protected def finishStop(): Unit = {
    val update: JmsConnectorState => JmsConnectorState = {
      case JmsConnectorStopping(completion) => JmsConnectorStopped(completion)
      case current => current
    }

    val previous = updateStateWith(update)
    connection(previous).foreach(_.close())
    connectionStateQueue.complete()
  }

  protected def publishAndFailStage(ex: Throwable): Unit = {
    val previous = updateState(JmsConnectorStopping(Failure(ex)))
    connection(previous).foreach(_.close())
    failStage(ex)
  }

  protected def updateState(next: JmsConnectorState): JmsConnectorState = {
    val update: JmsConnectorState => JmsConnectorState = {
      case current: JmsConnectorStopping => current
      case current: JmsConnectorStopped => current
      case _ => next
    }
    updateStateWith(update)
  }

  private def updateStateWith(f: JmsConnectorState => JmsConnectorState): JmsConnectorState = {
    val last = connectionState
    connectionState = f(last)

    // use type-based comparison to publish JmsConnectorInitializing only once.
    if (last.getClass != connectionState.getClass) {
      connectionStateQueue.offer(connectionState)
    }

    last
  }

  protected def connectionFailed(ex: Throwable): Unit = ex match {
    case ex: jms.JMSSecurityException =>
      log.error(ex,
                "{} initializing connection failed, security settings are not properly configured",
                attributes.nameOrDefault())
      publishAndFailStage(ex)

    case _: jms.JMSException | _: JmsConnectTimedOut => handleRetriableException(ex)

    case _ =>
      connectionState match {
        case _: JmsConnectorStopping | _: JmsConnectorStopped => logStoppingException(ex)
        case _ =>
          log.error(ex, "{} connection failed", attributes.nameOrDefault())
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
             attributes.nameOrDefault(),
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
              log.info("{} connected", attributes.nameOrDefault())
            }
          } else {
            updateState(init.copy(sessions = sessions + 1))
          }
        case s => ()
      }

    case Failure(ex: jms.JMSException) =>
      updateState(JmsConnectorDisconnected) match {
        case JmsConnectorInitializing(c, attempt, backoffMaxed, _) =>
          c.foreach(_.close())
          maybeReconnect(ex, attempt, backoffMaxed)
        case _ => ()
      }

    case Failure(ex) =>
      log.error(ex, "{} initializing connection failed", attributes.nameOrDefault())
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
      log.error(exception, "{} initializing connection failed", attributes.nameOrDefault())
      publishAndFailStage(exception)
    } else {
      val status = updateState(JmsConnectorDisconnected)
      connection(status).foreach(_.close())
      val delay = if (backoffMaxed) maxBackoff else waitTime(nextAttempt)
      val backoffNowMaxed = backoffMaxed || delay == maxBackoff
      scheduleOnce(AttemptConnect(nextAttempt, backoffNowMaxed), delay)
    }
  }

  override def onTimer(timerKey: Any): Unit = timerKey match {
    case AttemptConnect(attempt, backoffMaxed) =>
      log.info("{} retries connecting, attempt {}", attributes.nameOrDefault(), attempt)
      initSessionAsync(attempt, backoffMaxed)
    case _ => ()
  }

  protected def executionContext(attributes: Attributes): ExecutionContext = {
    val dispatcher = attributes.get[ActorAttributes.Dispatcher](
      ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
    ) match {
      case ActorAttributes.Dispatcher("") =>
        ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
      case d => d
    }

    ActorMaterializerHelper.downcast(materializer).system.dispatchers.lookup(dispatcher.dispatcher)
  }

  protected def createSession(connection: jms.Connection, createDestination: jms.Session => jms.Destination): S

  protected def initSessionAsync(attempt: Int = 0, backoffMaxed: Boolean = false): Unit = {
    val allSessions = openSessions(attempt, backoffMaxed)
    allSessions.failed.foreach(connectionFailedCB.invoke)(ExecutionContexts.sameThreadExecutionContext)
    // wait for all sessions to successfully initialize before invoking the onSession callback.
    // reduces flakiness (start, consume, then crash) at the cost of increased latency of startup.
    allSessions.foreach(_.foreach(onSession.invoke))
  }

  def startConnection: Boolean

  private def openSessions(attempt: Int, backoffMaxed: Boolean): Future[Seq[S]] = {
    val eventualConnection = openConnection(attempt, backoffMaxed)
    eventualConnection.flatMap { connection =>
      val sessionFutures =
        for (_ <- 0 until jmsSettings.sessionCount)
          yield Future(createSession(connection, destination.create))
      Future.sequence(sessionFutures)
    }(ExecutionContexts.sameThreadExecutionContext)
  }

  private def openConnection(attempt: Int, backoffMaxed: Boolean): Future[jms.Connection] = {
    implicit val system: ActorSystem = ActorMaterializerHelper.downcast(materializer).system
    val jmsConnection = openConnectionAttempt(startConnection)
    updateState(JmsConnectorInitializing(jmsConnection, attempt, backoffMaxed, 0))
    jmsConnection.map { connection =>
      connection.setExceptionListener(new jms.ExceptionListener {
        override def onException(ex: jms.JMSException): Unit = {
          Try(connection.close()) // best effort closing the connection.
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
        case Some(Credentials(username, password)) => factory.createConnection(username, password)
        case _ => factory.createConnection()
      }
      if (status.get == Connecting) { // `TimedOut` can be set at any point. So we have to check whether to continue.
        connectionRef.set(Some(connection))
        if (startConnection) connection.start()
      }
      // ... and close if the connection is not to be used, don't return the connection
      if (!status.compareAndSet(Connecting, Connected)) {
        connectionRef.get.foreach(_.close())
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
        connectionRef.get.foreach(_.close())
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

    Future.firstCompletedOf(Iterator(connectionFuture, timeoutFuture))(ExecutionContexts.sameThreadExecutionContext)
  }
}

/**
 * Internal API.
 */
@InternalApi
private[jms] object JmsConnector {

  sealed trait ConnectionAttemptStatus
  case object Connecting extends ConnectionAttemptStatus
  case object Connected extends ConnectionAttemptStatus
  case object TimedOut extends ConnectionAttemptStatus

  case class AttemptConnect(attempt: Int, backoffMaxed: Boolean)

  trait JmsConnectorState
  case object JmsConnectorDisconnected extends JmsConnectorState
  case class JmsConnectorInitializing(connection: Future[jms.Connection],
                                      attempt: Int,
                                      backoffMaxed: Boolean,
                                      sessions: Int)
      extends JmsConnectorState
  case class JmsConnectorConnected(connection: jms.Connection) extends JmsConnectorState
  case class JmsConnectorStopping(completion: Try[Done]) extends JmsConnectorState
  case class JmsConnectorStopped(completion: Try[Done]) extends JmsConnectorState

  def connection: JmsConnectorState => Future[jms.Connection] = {
    case JmsConnectorInitializing(c, _, _, _) => c
    case JmsConnectorConnected(c) => Future.successful(c)
    case _ => Future.failed(JmsNotConnected)
  }
}
