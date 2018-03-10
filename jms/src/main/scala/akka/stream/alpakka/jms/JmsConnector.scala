/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.ArrayBlockingQueue
import javax.jms
import javax.jms._

import akka.stream.ActorAttributes.Dispatcher
import akka.stream.ActorMaterializer
import akka.stream.stage.GraphStageLogic

import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic =>

  implicit private[jms] var ec: ExecutionContext = _

  private[jms] var jmsConnection: Option[Connection] = None

  private[jms] var jmsSessions = Seq.empty[JmsSession]

  private[jms] def jmsSettings: JmsSettings

  private[jms] def onSessionOpened(jmsSession: JmsSession): Unit = {}

  private[jms] def fail = getAsyncCallback[Throwable](e => failStage(e))

  private def onConnection = getAsyncCallback[Connection] { c =>
    jmsConnection = Some(c)
  }

  private def onSession =
    getAsyncCallback[JmsSession] { session =>
      jmsSessions :+= session
      onSessionOpened(session)
    }

  private[jms] def initSessionAsync(dispatcher: Dispatcher): Future[Unit] = {
    ec = materializer match {
      case m: ActorMaterializer => m.system.dispatchers.lookup(dispatcher.dispatcher)
      case x => throw new IllegalArgumentException(s"Stage only works with the ActorMaterializer, was: $x")
    }
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

  private[jms] def createSession(connection: Connection, createDestination: jms.Session => jms.Destination): JmsSession

  private[jms] def openSessions(): Seq[JmsSession] = {
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
      case Some(Queue(name)) =>
        session: Session =>
          session.createQueue(name)
      case Some(Topic(name)) =>
        session: Session =>
          session.createTopic(name)
      case _ => throw new IllegalArgumentException("Destination is missing")
    }

    val sessionCount = jmsSettings match {
      case settings: JmsConsumerSettings =>
        settings.sessionCount
      case _ => 1
    }

    0 until sessionCount map { _ =>
      createSession(connection, createDestination)
    }
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

  override def closeSession(): Unit = stopMessageListenerAndCloseSession

  override def abortSession(): Unit = stopMessageListenerAndCloseSession

  private def stopMessageListenerAndCloseSession(): Unit = {
    ackQueue.put(() => throw StopMessageListenerException())
    session.close()
  }
}

private[jms] class JmsTxSession(override val connection: jms.Connection,
                                override val session: jms.Session,
                                override val destination: jms.Destination)
    extends JmsSession(connection, session, destination) {

  private[jms] val commitQueue = new ArrayBlockingQueue[() => Unit](1)

  def commit(): Unit = commitQueue.put(session.commit _)

  def rollback(): Unit = commitQueue.put(session.rollback _)

  override def abortSession(): Unit = {
    rollback()
    session.close()
  }
}
