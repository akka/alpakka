/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
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
 * Internal API
 */
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

private[jms] trait JmsConsumerConnector extends JmsConnector[JmsConsumerSession] {
  this: TimerGraphStageLogic with StageLogging =>

  override val startConnection = true

  protected def createSession(connection: jms.Connection,
                              createDestination: jms.Session => jms.Destination): JmsConsumerSession

}

private[jms] trait JmsProducerConnector extends JmsConnector[JmsProducerSession] {
  this: TimerGraphStageLogic with StageLogging =>

  protected final def createSession(connection: jms.Connection,
                                    createDestination: jms.Session => jms.Destination): JmsProducerSession = {
    val session = connection.createSession(false, AcknowledgeMode.AutoAcknowledge.mode)
    new JmsProducerSession(connection, session, createDestination(session))
  }

  override val startConnection = false

  val status: JmsProducerMatValue = new JmsProducerMatValue {
    override def connected: Source[JmsConnectorState, NotUsed] =
      Source.fromFuture(connectionStateSource).flatMapConcat(identity)
  }
}

private[jms] object JmsMessageProducer {
  def apply(jmsSession: JmsProducerSession, settings: JmsProducerSettings, epoch: Int): JmsMessageProducer = {
    val producer = jmsSession.session.createProducer(null)
    if (settings.timeToLive.nonEmpty) {
      producer.setTimeToLive(settings.timeToLive.get.toMillis)
    }
    new JmsMessageProducer(producer, jmsSession, epoch)
  }
}

private[jms] class JmsMessageProducer(jmsProducer: jms.MessageProducer,
                                      jmsSession: JmsProducerSession,
                                      val epoch: Int) {

  private val defaultDestination = jmsSession.jmsDestination

  private val destinationCache = new SoftReferenceCache[Destination, jms.Destination]()

  def send(elem: JmsMessage): Unit = {
    val message: jms.Message = createMessage(elem)
    populateMessageProperties(message, elem)

    val (sendHeaders, headersBeforeSend: Set[JmsHeader]) = elem.headers.partition(_.usedDuringSend)
    populateMessageHeader(message, headersBeforeSend)

    val deliveryMode = sendHeaders
      .collectFirst { case x: JmsDeliveryMode => x.deliveryMode }
      .getOrElse(jmsProducer.getDeliveryMode)

    val priority = sendHeaders
      .collectFirst { case x: JmsPriority => x.priority }
      .getOrElse(jmsProducer.getPriority)

    val timeToLive = sendHeaders
      .collectFirst { case x: JmsTimeToLive => x.timeInMillis }
      .getOrElse(jmsProducer.getTimeToLive)

    val destination = elem.destination match {
      case Some(messageDestination) => lookup(messageDestination)
      case None => defaultDestination
    }
    jmsProducer.send(destination, message, deliveryMode, priority, timeToLive)
  }

  private def lookup(dest: Destination) = destinationCache.lookup(dest, dest.create(jmsSession.session))

  private[jms] def createMessage(element: JmsMessage): jms.Message =
    element match {

      case textMessage: JmsTextMessage => jmsSession.session.createTextMessage(textMessage.body)

      case byteMessage: JmsByteMessage =>
        val newMessage = jmsSession.session.createBytesMessage()
        newMessage.writeBytes(byteMessage.bytes)
        newMessage

      case mapMessage: JmsMapMessage =>
        val newMessage = jmsSession.session.createMapMessage()
        populateMapMessage(newMessage, mapMessage)
        newMessage

      case objectMessage: JmsObjectMessage => jmsSession.session.createObjectMessage(objectMessage.serializable)

    }

  private[jms] def populateMessageProperties(message: javax.jms.Message, jmsMessage: JmsMessage): Unit =
    jmsMessage.properties.foreach {
      case (key, v) =>
        v match {
          case v: String => message.setStringProperty(key, v)
          case v: Int => message.setIntProperty(key, v)
          case v: Boolean => message.setBooleanProperty(key, v)
          case v: Byte => message.setByteProperty(key, v)
          case v: Short => message.setShortProperty(key, v)
          case v: Long => message.setLongProperty(key, v)
          case v: Double => message.setDoubleProperty(key, v)
          case null => throw NullMessageProperty(key, jmsMessage)
          case _ => throw UnsupportedMessagePropertyType(key, v, jmsMessage)
        }
    }

  private def populateMapMessage(message: javax.jms.MapMessage, jmsMessage: JmsMapMessage): Unit =
    jmsMessage.body.foreach {
      case (key, v) =>
        v match {
          case v: String => message.setString(key, v)
          case v: Int => message.setInt(key, v)
          case v: Boolean => message.setBoolean(key, v)
          case v: Byte => message.setByte(key, v)
          case v: Short => message.setShort(key, v)
          case v: Long => message.setLong(key, v)
          case v: Double => message.setDouble(key, v)
          case v: Array[Byte] => message.setBytes(key, v)
          case null => throw NullMapMessageEntry(key, jmsMessage)
          case _ => throw UnsupportedMapMessageEntryType(key, v, jmsMessage)
        }
    }

  private def populateMessageHeader(message: javax.jms.Message, headers: Set[JmsHeader]): Unit =
    headers.foreach {
      case JmsType(jmsType) => message.setJMSType(jmsType)
      case JmsReplyTo(destination) => message.setJMSReplyTo(destination.create(jmsSession.session))
      case JmsCorrelationId(jmsCorrelationId) => message.setJMSCorrelationID(jmsCorrelationId)
    }
}

private[jms] sealed trait JmsSession {

  def connection: jms.Connection

  def session: jms.Session

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { closeSession() }

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { abortSession() }

  private[jms] def abortSession(): Unit = closeSession()
}

private[jms] class JmsProducerSession(val connection: jms.Connection,
                                      val session: jms.Session,
                                      val jmsDestination: jms.Destination)
    extends JmsSession

private[jms] class JmsConsumerSession(val connection: jms.Connection,
                                      val session: jms.Session,
                                      val jmsDestination: jms.Destination,
                                      val settingsDestination: Destination)
    extends JmsSession {

  private[jms] def createConsumer(
      selector: Option[String]
  )(implicit ec: ExecutionContext): Future[jms.MessageConsumer] =
    Future {
      (selector, settingsDestination) match {
        case (None, t: DurableTopic) =>
          session.createDurableSubscriber(jmsDestination.asInstanceOf[jms.Topic], t.subscriberName)

        case (Some(expr), t: DurableTopic) =>
          session.createDurableSubscriber(jmsDestination.asInstanceOf[jms.Topic], t.subscriberName, expr, false)

        case (Some(expr), _) =>
          session.createConsumer(jmsDestination, expr)

        case (None, _) =>
          session.createConsumer(jmsDestination)
      }
    }
}

private[jms] class JmsAckSession(override val connection: jms.Connection,
                                 override val session: jms.Session,
                                 override val jmsDestination: jms.Destination,
                                 override val settingsDestination: Destination,
                                 val maxPendingAcks: Int)
    extends JmsConsumerSession(connection, session, jmsDestination, settingsDestination) {

  private[jms] var pendingAck = 0
  private[jms] val ackQueue = new ArrayBlockingQueue[() => Unit](maxPendingAcks + 1)

  def ack(message: jms.Message): Unit = ackQueue.put(message.acknowledge _)

  override def closeSession(): Unit = stopMessageListenerAndCloseSession()

  override def abortSession(): Unit = stopMessageListenerAndCloseSession()

  private def stopMessageListenerAndCloseSession(): Unit = {
    ackQueue.put(() => throw StopMessageListenerException())
    session.close()
  }
}
