/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package jakartajmstestkit

import java.util.{Collections, UUID}

import jakarta.jms.{ConnectionFactory, QueueConnectionFactory, TextMessage}

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * This testkit was copied from https://github.com/sullis/jms-testkit with modifications
 * to support Jakarta Messaging. Replacing `javax.jms` with `jakarta.jms`.
 * ActiveMQ replaced with Artemis EmbeddedActiveMQ.
 * jms-testkit is licensed under the Apache License, Version 2.0.
 */
class JmsQueue(val broker: JmsBroker) {

  val queueName: String = "Queue-" + UUID.randomUUID.toString

  def size: Long = calculateQueueSize(queueName)

  def createQueueConnectionFactory: QueueConnectionFactory = broker.createQueueConnectionFactory
  def createConnectionFactory: ConnectionFactory = broker.createConnectionFactory

  private def calculateQueueSize(qName: String): Long = {
    broker.getQueueSize(qName)
  }

  def toSeq: Seq[String] = {
    val qconn = createQueueConnectionFactory.createQueueConnection()
    val session = qconn.createQueueSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(queueName)
    qconn.start
    val browser = session.createBrowser(queue)
    val result =
      Collections.list(browser.getEnumeration).asScala.asInstanceOf[Iterable[TextMessage]].map(_.getText).toSeq
    Try { browser.close() }
    Try { session.close() }
    Try { qconn.close() }
    result
  }

  def toJavaList: java.util.List[String] = {
    java.util.Collections.unmodifiableList(toSeq.asJava)
  }

  def publishMessage(msg: String): Unit = {
    val qconn = createQueueConnectionFactory.createQueueConnection()
    val session = qconn.createQueueSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(queueName)
    val sender = session.createSender(queue)
    sender.send(session.createTextMessage(msg))
    Try { sender.close() }
    Try { session.close() }
    Try { qconn.close() }
  }

  override def toString(): String = {
    getClass.getSimpleName + s"[${queueName}]"
  }
}

object JmsQueue {
  def apply(): JmsQueue = {
    new JmsQueue(JmsBroker())
  }
}
