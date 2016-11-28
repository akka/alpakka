/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms
import javax.jms.{ ExceptionListener, JMSException }

import akka.stream.ActorMaterializer
import akka.stream.stage.{ GraphStageLogic, StageLogging }

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Internal API
 */
private[jms] trait JmsConnector { this: GraphStageLogic with StageLogging =>

  implicit private[jms] var ec: ExecutionContext = _

  private[jms] var jmsSession: JmsSession = _

  private[jms] def jmsSettings: JmsSettings

  private[jms] def onSessionOpened(): Unit

  private[jms] def fail = getAsyncCallback[JMSException](e => failStage(e))

  private def onSession =
    getAsyncCallback[JmsSession](session => {
      jmsSession = session
      onSessionOpened()
    })

  override def preStart(): Unit = {
    ec = materializer match {
      case m: ActorMaterializer => m.system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
      case x => throw new IllegalArgumentException(s"Stage only works with the ActorMaterializer, was: $x")
    }
    val factory = jmsSettings.connectionFactory
    Future {
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
      onSession.invoke(JmsSession(connection, session, dest))
    }
  }

  override def postStop(): Unit =
    jmsSession.closeSession().onFailure {
      case e => log.error(e, "Error closing connection")
    }
}

private[jms] case class JmsSession(connection: jms.Connection, session: jms.Session, destination: jms.Destination) {

  private[jms] def closeSession()(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      try {
        session.close()
      } finally {
        connection.close()
      }
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
