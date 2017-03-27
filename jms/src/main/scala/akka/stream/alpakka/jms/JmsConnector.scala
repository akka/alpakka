/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms
import javax.jms.{ExceptionListener, JMSException}

import akka.stream.ActorAttributes.Dispatcher
import akka.stream.ActorMaterializer
import akka.stream.stage.GraphStageLogic

import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic =>

  implicit private[jms] var ec: ExecutionContext = _

  private[jms] var jmsSession: JmsSession = _

  private[jms] def jmsSettings: JmsSettings

  private[jms] def onSessionOpened(): Unit = {}

  private[jms] def fail = getAsyncCallback[Throwable](e => failStage(e))

  private def onSession =
    getAsyncCallback[JmsSession](session => {
      jmsSession = session
      onSessionOpened()
    })

  private[jms] def initSessionAsync(dispatcher: Dispatcher) = {
    ec = materializer match {
      case m: ActorMaterializer => m.system.dispatchers.lookup(dispatcher.dispatcher)
      case x => throw new IllegalArgumentException(s"Stage only works with the ActorMaterializer, was: $x")
    }
    Future {
      val session = openSession()
      onSession.invoke(session)
    }.onFailure {
      case e: Exception => fail.invoke(e)
    }
  }

  private[jms] def openSession(): JmsSession = {
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
    val session = connection.createSession(false, jms.Session.CLIENT_ACKNOWLEDGE)
    val dest = jmsSettings.destination match {
      case Some(Queue(name)) => session.createQueue(name)
      case Some(Topic(name)) => session.createTopic(name)
      case _ => throw new IllegalArgumentException("Destination is missing")
    }
    JmsSession(connection, session, dest)
  }
}

private[jms] case class JmsSession(connection: jms.Connection, session: jms.Session, destination: jms.Destination) {

  private[jms] def closeSessionAsync()(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      closeSession()
    }

  private[jms] def closeSession(): Unit =
    try {
      session.close()
    } finally {
      connection.close()
    }

  private[jms] def createProducer()(implicit ec: ExecutionContext): Future[jms.MessageProducer] =
    Future {
      session.createProducer(destination)
    }

  private[jms] def createConsumer()(implicit ec: ExecutionContext): Future[jms.MessageConsumer] =
    Future {
      session.createConsumer(destination)
    }
}
