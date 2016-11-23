/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms
import javax.jms._

import akka.stream.stage.{ GraphStageLogic, StageLogging }

trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
}

sealed trait Destination
case class Topic(name: String) extends Destination
case class Queue(name: String) extends Destination

object JmsSourceSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSourceSettings(connectionFactory)

}

case class JmsSourceSettings(connectionFactory: ConnectionFactory,
                             destination: Option[Destination] = None,
                             credentials: Option[Credentials] = None,
                             bufferSize: Int = 100)
    extends JmsSettings {
  def withCredential(credentials: Credentials) = copy(credentials = Some(credentials))
  def withBufferSize(size: Int) = copy(bufferSize = size)
  def withQueue(name: String) = copy(destination = Some(Queue(name)))
  def withTopic(name: String) = copy(destination = Some(Topic(name)))
}

object JmsSinkSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSinkSettings(connectionFactory)

}

case class JmsSinkSettings(connectionFactory: ConnectionFactory,
                           destination: Option[Destination] = None,
                           credentials: Option[Credentials] = None)
    extends JmsSettings {
  def withCredential(credentials: Credentials) = copy(credentials = Some(credentials))
  def withQueue(name: String) = copy(destination = Some(Queue(name)))
  def withTopic(name: String) = copy(destination = Some(Topic(name)))
}

case class Credentials(username: String, password: String)

trait JmsConnector { this: GraphStageLogic with StageLogging =>

  private[jms] var jmsConnection: Option[Connection] = None
  private[jms] var jmsSession: Option[Session] = None
  private[jms] var jmsDestination: Option[jms.Destination] = None

  def jmsSettings: JmsSettings

  def fail = getAsyncCallback[JMSException](e => failStage(e))

  def openQueue(): Unit = {
    val factory: ConnectionFactory = jmsSettings.connectionFactory
    try {
      jmsConnection = Option(jmsSettings.credentials match {
        case Some(Credentials(username, password)) => factory.createConnection(username, password)
        case _ => factory.createConnection()
      })
      jmsConnection.foreach(_.setExceptionListener(new ExceptionListener {
        override def onException(exception: JMSException): Unit = {
          log.error(exception, "Error on jms connexion")
          fail.invoke(exception)
        }
      }))
      jmsConnection.foreach(_.start)
    } catch {
      case e: JMSException =>
        log.error(e, "Error opening connection")
        failStage(e)
    }
    try {
      jmsSession = jmsConnection.map(_.createSession(false, Session.CLIENT_ACKNOWLEDGE))
    } catch {
      case e: JMSException =>
        log.error(e, "Error opening session")
        failStage(e)
    }
    try {
      jmsDestination = jmsSettings.destination match {
        case Some(Queue(name)) => jmsSession.map(_.createQueue(name))
        case Some(Topic(name)) => jmsSession.map(_.createTopic(name))
        case _ =>
          failStage(new IllegalArgumentException("Missing queue or topic name"))
          None
      }

    } catch {
      case e: JMSException =>
        log.error(e, "Error creating queue")
        failStage(e)
    }
  }

  def closeConnection(): Unit = {
    try {
      jmsSession.foreach(_.close)
      jmsSession = None
    } catch {
      case e: JMSException =>
        log.error("Error closing session", e)
    }
    try {
      jmsConnection.foreach(_.close)
      jmsConnection = None
    } catch {
      case e: JMSException =>
        log.error("Error closing connection", e)
    }
  }

  override def preStart(): Unit =
    openQueue()

  override def postStop(): Unit =
    closeConnection()
}
