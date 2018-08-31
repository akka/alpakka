/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.ArrayBlockingQueue

import javax.jms
import javax.jms._
import akka.stream.{ActorAttributes, ActorMaterializer, Attributes}
import akka.stream.stage.{AsyncCallback, GraphStageLogic}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic =>

  implicit protected var ec: ExecutionContext = _

  protected var jmsConnection: Option[Connection] = None

  protected var jmsSessions = Seq.empty[JmsSession]

  protected def jmsSettings: JmsSettings

  protected def onSessionOpened(jmsSession: JmsSession): Unit = {}

  protected def fail: AsyncCallback[Throwable] = getAsyncCallback[Throwable](e => failStage(e))

  private def onConnection = getAsyncCallback[Connection] { c =>
    jmsConnection = Some(c)
  }

  private def onSession =
    getAsyncCallback[JmsSession] { session =>
      jmsSessions :+= session
      onSessionOpened(session)
    }

  protected def executionContext(attributes: Attributes): ExecutionContext = {
    val dispatcher = attributes.get[ActorAttributes.Dispatcher](
      ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
    ) match {
      case ActorAttributes.Dispatcher("") =>
        ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
      case d => d
    }

    materializer match {
      case m: ActorMaterializer => m.system.dispatchers.lookup(dispatcher.dispatcher)
      case x => throw new IllegalArgumentException(s"Stage only works with the ActorMaterializer, was: $x")
    }
  }

  protected def initSessionAsync(executionContext: ExecutionContext): Future[Unit] = {
    ec = executionContext
    val future = Future {
      val sessions = openSessions()
      sessions foreach { session =>
        onSession.invoke(session)
      }
    }
    future.onFailure {
      case e: Exception => fail.invoke(e)
    }
    future
  }

  protected def createSession(connection: Connection, createDestination: jms.Session => jms.Destination): JmsSession

  protected def openSessions(): Seq[JmsSession] = {
    val factory = jmsSettings.connectionFactory
    val connection = jmsSettings.credentials match {
      case Some(Credentials(username, password)) => factory.createConnection(username, password)
      case _ => factory.createConnection()
    }
    connection.setExceptionListener(new ExceptionListener {
      override def onException(exception: JMSException) =
        fail.invoke(exception)
    })
    connection.start()
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

private[jms] object JmsMessageProducer {
  def apply(jmsSession: JmsSession, settings: JmsProducerSettings): JmsMessageProducer = {
    val producer = jmsSession.session.createProducer(jmsSession.destination)
    if (settings.timeToLive.nonEmpty) {
      producer.setTimeToLive(settings.timeToLive.get.toMillis)
    }
    new JmsMessageProducer(producer, jmsSession)
  }
}

private[jms] class JmsMessageProducer(jmsProducer: MessageProducer, jmsSession: JmsSession) {

  def send(elem: JmsMessage): Unit = {
    val message: Message = createMessage(elem)
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

  private[jms] def createMessage(element: JmsMessage): Message =
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
                              val destination: jms.Destination) {

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { closeSession() }

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { abortSession() }

  private[jms] def abortSession(): Unit = closeSession()

  private[jms] def createProducer()(implicit ec: ExecutionContext): Future[jms.MessageProducer] =
    Future {
      session.createProducer(destination)
    }

  private[jms] def createConsumer(
      selector: Option[String]
  )(implicit ec: ExecutionContext): Future[jms.MessageConsumer] =
    Future {
      selector match {
        case None => session.createConsumer(destination)
        case Some(expr) => session.createConsumer(destination, expr)
      }
    }
}

private[jms] class JmsAckSession(override val connection: jms.Connection,
                                 override val session: jms.Session,
                                 override val destination: jms.Destination,
                                 val maxPendingAcks: Int)
    extends JmsSession(connection, session, destination) {

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
                                override val destination: jms.Destination)
    extends JmsSession(connection, session, destination) {

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
