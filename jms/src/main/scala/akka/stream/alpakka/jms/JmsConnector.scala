/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

import akka.stream.alpakka.jms.JmsMessageProducer.{DestinationMode, MessageDefinedDestination}
import javax.jms
import javax.jms._
import akka.stream.{ActorAttributes, ActorMaterializer, Attributes}
import akka.stream.stage.{AsyncCallback, GraphStageLogic}

import scala.concurrent.{ExecutionContext, Future}
import scala.ref.SoftReference
import scala.collection.JavaConverters._

/**
 * Internal API
 */
private[jms] trait JmsConnector[S <: JmsSession] { this: GraphStageLogic =>

  implicit protected var ec: ExecutionContext = _

  protected var jmsConnection: Option[Connection] = None

  protected var jmsSessions = Seq.empty[S]

  protected def jmsSettings: JmsSettings

  protected def onSessionOpened(jmsSession: S): Unit = {}

  protected val fail: AsyncCallback[Throwable] = getAsyncCallback[Throwable](e => failStage(e))

  private val onConnection: AsyncCallback[Connection] = getAsyncCallback[Connection] { c =>
    jmsConnection = Some(c)
  }

  private val onSession: AsyncCallback[S] = getAsyncCallback[S] { session =>
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
    future.failed.foreach(e => fail.invoke(e))
    future
  }

  def openSessions(): Seq[S]

  def openConnection(): Connection = {
    val factory = jmsSettings.connectionFactory
    val connection = jmsSettings.credentials match {
      case Some(Credentials(username, password)) => factory.createConnection(username, password)
      case _ => factory.createConnection()
    }
    connection.setExceptionListener(new ExceptionListener {
      override def onException(exception: JMSException): Unit =
        fail.invoke(exception)
    })
    onConnection.invoke(connection)
    connection
  }
}

private[jms] trait JmsConsumerConnector extends JmsConnector[JmsConsumerSession] { this: GraphStageLogic =>

  protected def createSession(connection: Connection,
                              createDestination: jms.Session => jms.Destination): JmsConsumerSession

  def openSessions(): Seq[JmsConsumerSession] = {
    val connection = openConnection()
    connection.start()

    val createDestination = jmsSettings.destination match {
      case Some(destination) => destination.create
      case _ => throw new IllegalArgumentException("Destination is missing")
    }

    for (_ <- 0 until jmsSettings.sessionCount)
      yield createSession(connection, createDestination)
  }
}

private[jms] trait JmsProducerConnector extends JmsConnector[JmsProducerSession] { this: GraphStageLogic =>

  def openSessions(): Seq[JmsProducerSession] = {
    val connection = openConnection()

    val maybeDestinationFactory = jmsSettings.destination.map(_.create)

    for (_ <- 0 until jmsSettings.sessionCount)
      yield {
        val session = connection.createSession(false, AcknowledgeMode.AutoAcknowledge.mode)
        new JmsProducerSession(connection, session, maybeDestinationFactory.map(_.apply(session)))
      }
  }
}

private[jms] object JmsMessageProducer {
  def apply(jmsSession: JmsProducerSession, settings: JmsProducerSettings): JmsMessageProducer = {
    val producer = jmsSession.session.createProducer(jmsSession.jmsDestination.orNull)
    if (settings.timeToLive.nonEmpty) {
      producer.setTimeToLive(settings.timeToLive.get.toMillis)
    }
    new JmsMessageProducer(producer, jmsSession, destinationMode(settings))
  }

  def destinationMode(settings: JmsProducerSettings): DestinationMode =
    if (settings.destination.isDefined) ProducerDefinedDestination else MessageDefinedDestination

  sealed trait DestinationMode
  object ProducerDefinedDestination extends DestinationMode
  object MessageDefinedDestination extends DestinationMode

}

private[jms] class JmsMessageProducer(jmsProducer: MessageProducer,
                                      jmsSession: JmsProducerSession,
                                      mode: DestinationMode) {

  // Using a synchronized map (and not ConcurrentHashMap) for the cache since:
  // - we run on our on execution context and cannot piggy-back on Akka's synchronization
  // - we run send()s with one thread at a time, so there will be no lock contention.
  private val destinationCache =
    Collections.synchronizedMap(new util.HashMap[Destination, SoftReference[jms.Destination]]()).asScala

  def send(elem: JmsMessage): Unit = {
    val message: Message = createMessage(elem)
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

    elem match {
      case directed: JmsDirectedMessage if mode == MessageDefinedDestination =>
        val jmsDestination = lookup(directed.destination)
        jmsProducer.send(jmsDestination, message, deliveryMode, priority, timeToLive)
      case _ =>
        jmsProducer.send(message, deliveryMode, priority, timeToLive)
    }
  }

  def lookup(destination: Destination): jms.Destination =
    destinationCache.get(destination) match {
      case Some(SoftReference(jmsDestination)) =>
        jmsDestination
      case _ =>
        val jmsDestination = destination.create(jmsSession.session)
        destinationCache.put(destination, SoftReference(jmsDestination))
        jmsDestination
    }

  private[jms] def createMessage(element: JmsMessage): Message =
    element match {

      case textMessage: JmsAbstractTextMessage => jmsSession.session.createTextMessage(textMessage.body)

      case byteMessage: JmsAbstractByteMessage =>
        val newMessage = jmsSession.session.createBytesMessage()
        newMessage.writeBytes(byteMessage.bytes)
        newMessage

      case mapMessage: JmsAbstractMapMessage =>
        val newMessage = jmsSession.session.createMapMessage()
        populateMapMessage(newMessage, mapMessage)
        newMessage

      case objectMessage: JmsAbstractObjectMessage => jmsSession.session.createObjectMessage(objectMessage.serializable)

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

  private def populateMapMessage(message: javax.jms.MapMessage, jmsMessage: JmsAbstractMapMessage): Unit =
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

private[jms] trait JmsSession {

  def connection: jms.Connection

  def session: jms.Session

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { closeSession() }

  private[jms] def closeSession(): Unit = session.close()

  private[jms] def abortSessionAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { abortSession() }

  private[jms] def abortSession(): Unit = closeSession()
}

private[jms] class JmsProducerSession(val connection: jms.Connection,
                                      val session: jms.Session,
                                      val jmsDestination: Option[jms.Destination])
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

private[jms] class JmsTxSession(override val connection: jms.Connection,
                                override val session: jms.Session,
                                override val jmsDestination: jms.Destination,
                                override val settingsDestination: Destination)
    extends JmsConsumerSession(connection, session, jmsDestination, settingsDestination) {

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
