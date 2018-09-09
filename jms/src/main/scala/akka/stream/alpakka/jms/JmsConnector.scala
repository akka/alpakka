/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import akka.stream.{ActorAttributes, ActorMaterializerHelper, Attributes}
import javax.jms

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic =>

  implicit protected var ec: ExecutionContext = _

  protected var jmsConnection: Option[jms.Connection] = None

  protected var jmsSessions = Seq.empty[JmsSession]

  protected def jmsSettings: JmsSettings

  protected def fail: AsyncCallback[Throwable] = getAsyncCallback[Throwable](e => failStage(e))

  private def onConnection = getAsyncCallback[jms.Connection] { c =>
    jmsConnection = Some(c)
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

  protected def createSession(connection: jms.Connection, createDestination: jms.Session => jms.Destination): JmsSession

  sealed trait ConnectionStatus
  case object Connecting extends ConnectionStatus
  case object Connected extends ConnectionStatus
  case object TimedOut extends ConnectionStatus

  private def startConnection()(implicit system: ActorSystem): Future[jms.Connection] = {
    val factory = jmsSettings.connectionFactory
    val connectionRef = new AtomicReference[Option[jms.Connection]](None)

    // status is also the decision point between the two futures below which one will win.
    val status = new AtomicReference[ConnectionStatus](Connecting)

    val connectionFuture = Future {
      val connection = jmsSettings.credentials match {
        case Some(Credentials(username, password)) => factory.createConnection(username, password)
        case _ => factory.createConnection()
      }
      if (status.get == Connecting) { // `TimedOut` can be set at any point. So we have to check whether to continue.
        connectionRef.set(Some(connection))
        connection.start()
      }
      // ... and close if the connection is not to be used, don't return the connection
      if (!status.compareAndSet(Connecting, Connected)) {
        connectionRef.get.foreach(_.close())
        connectionRef.set(None)
        throw new TimeoutException("Received timed out signal trying to establish connection")
      } else connection
    }

    val timeoutFuture = after(jmsSettings.connectionRetrySettings.connectTimeout, system.scheduler) {
      // Even if the timer goes off, the connection may already be good. We use the
      // status field and an atomic compareAndSet to see whether we should indeed time out, or just return
      // the connection. In this case it does not matter which future returns. Both will have the right answer.
      if (status.compareAndSet(Connecting, TimedOut)) {
        connectionRef.get.foreach(_.close())
        connectionRef.set(None)
        Future.failed(new TimeoutException("Timed out trying to establish connection"))
      } else
        connectionRef.get match {
          case Some(connection) => Future.successful(connection)
          case None => Future.failed(new IllegalStateException("BUG: Connection reference not set when connected"))
        }
    }

    Future.firstCompletedOf(Iterator(connectionFuture, timeoutFuture))
  }

  private def startConnectionWithRetry(n: Int = 0,
                                       maxed: Boolean = false)(implicit system: ActorSystem): Future[jms.Connection] =
    startConnection().recoverWith {
      case e: jms.JMSSecurityException => Future.failed(e)
      case NonFatal(t) =>
        val retrySettings = jmsSettings.connectionRetrySettings
        import retrySettings._
        val nextN = n + 1
        if (maxRetries >= 0 && nextN > maxRetries) { // Negative maxRetries treated as infinite.
          if (maxRetries == 0) Future.failed(t)
          else Future.failed(ConnectionRetryException(s"Could not establish connection after $n retries.", t))
        } else {
          val delay = if (maxed) maxBackoff else waitTime(nextN)
          if (delay >= maxBackoff) {
            after(maxBackoff, system.scheduler) {
              startConnectionWithRetry(nextN, maxed = true)
            }
          } else {
            after(delay, system.scheduler) {
              startConnectionWithRetry(nextN)
            }
          }
        }
    }

  private[jms] def createConnectionAndSessions(
      onConnectionFailure: jms.JMSException => Unit
  ): Future[Seq[JmsSession]] = {
    implicit val system: ActorSystem = ActorMaterializerHelper.downcast(materializer).system
    startConnectionWithRetry().map { connection =>
      connection.setExceptionListener(new jms.ExceptionListener {
        override def onException(ex: jms.JMSException) = {
          try {
            connection.close() // best effort closing the connection.
          } catch {
            case _: Throwable =>
          }
          jmsSessions = Seq.empty

          onConnectionFailure(ex)
        }
      })
      onConnection.invoke(connection)

      val createDestination = jmsSettings.destination match {
        case Some(destination) =>
          destination.create
        case _ => throw new IllegalArgumentException("Destination is missing")
      }

      0 until jmsSettings.sessionCount map { _ =>
        createSession(connection, createDestination)
      }
    }
  }
}

private[jms] object JmsMessageProducer {
  def apply(jmsSession: JmsSession, settings: JmsProducerSettings): JmsMessageProducer = {
    val producer = jmsSession.session.createProducer(jmsSession.jmsDestination)
    if (settings.timeToLive.nonEmpty) {
      producer.setTimeToLive(settings.timeToLive.get.toMillis)
    }
    new JmsMessageProducer(producer, jmsSession)
  }
}

private[jms] class JmsMessageProducer(jmsProducer: jms.MessageProducer, jmsSession: JmsSession) {

  def send(elem: JmsMessage): Unit = {
    val message: jms.Message = createMessage(elem)
    populateMessageProperties(message, elem)

    val (sendHeaders, headersBeforeSend: Set[JmsHeader]) = elem.headers.partition(_.usedDuringSend)
    populateMessageHeader(message, headersBeforeSend)

    val deliveryModeOption = findHeader(sendHeaders) { case x: JmsDeliveryMode => x.deliveryMode }
    val priorityOption = findHeader(sendHeaders) { case x: JmsPriority => x.priority }
    val timeToLiveInMillisOption = findHeader(sendHeaders) { case x: JmsTimeToLive => x.timeInMillis }

    jmsProducer.send(
      message,
      deliveryModeOption.getOrElse(jmsProducer.getDeliveryMode),
      priorityOption.getOrElse(jmsProducer.getPriority),
      timeToLiveInMillisOption.getOrElse(jmsProducer.getTimeToLive)
    )
  }

  private def findHeader[T](headersDuringSend: Set[JmsHeader])(f: PartialFunction[JmsHeader, T]): Option[T] =
    headersDuringSend.collectFirst(f)

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
    jmsMessage.properties().foreach {
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

private[jms] class JmsSession(val connection: jms.Connection,
                              val session: jms.Session,
                              val jmsDestination: jms.Destination,
                              val settingsDestination: Destination) {

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { closeSession() }

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { abortSession() }

  private[jms] def abortSession(): Unit = closeSession()

  private[jms] def createProducer()(implicit ec: ExecutionContext): Future[jms.MessageProducer] =
    Future {
      session.createProducer(jmsDestination)
    }

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
    extends JmsSession(connection, session, jmsDestination, settingsDestination) {

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

private[jms] class JmsTxSession(override val connection: jms.Connection,
                                override val session: jms.Session,
                                override val jmsDestination: jms.Destination,
                                override val settingsDestination: Destination)
    extends JmsSession(connection, session, jmsDestination, settingsDestination) {

  private[jms] val commitQueue = new ArrayBlockingQueue[TxEnvelope => Unit](1)

  def commit(commitEnv: TxEnvelope): Unit = commitQueue.put { srcEnv =>
    require(srcEnv == commitEnv, s"Source envelope mismatch on commit. Source: $srcEnv Commit: $commitEnv")
    session.commit()
  }

  def rollback(commitEnv: TxEnvelope): Unit = commitQueue.put { srcEnv =>
    require(srcEnv == commitEnv, s"Source envelope mismatch on rollback. Source: $srcEnv Commit: $commitEnv")
    session.rollback()
  }

  override def abortSession(): Unit = {
    // On abort, tombstone the onMessage loop to stop processing messages even if more messages are delivered.
    commitQueue.put(_ => throw StopMessageListenerException())
    session.close()
  }
}
