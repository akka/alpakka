/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package jakartajmstestkit

import java.util.UUID
import jakarta.jms.{ConnectionFactory, Message, MessageListener, TextMessage, TopicConnectionFactory}
import scala.util.Try
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * This testkit was copied from https://github.com/sullis/jms-testkit with modifications
 * to support Jakarta Messaging. Replacing `javax.jms` with `jakarta.jms`.
 * jms-testkit is licensed under the Apache License, Version 2.0.
 */
class JmsTopic(val broker: JmsBroker) {
  val topicName: String = "Topic-" + UUID.randomUUID.toString

  private val mutableList = ListBuffer[String]()

  registerMessageListener()

  private def registerMessageListener(): Unit = {
    val conn = this.createTopicConnectionFactory.createTopicConnection()
    conn.start()
    val session = conn.createTopicSession(true, jakarta.jms.Session.AUTO_ACKNOWLEDGE)
    val topic = session.createTopic(topicName)
    session
      .createSubscriber(topic)
      .setMessageListener(new MessageListener() {
        override def onMessage(message: Message): Unit = {
          mutableList += message.asInstanceOf[TextMessage].getText
        }
      })
  }

  def createTopicConnectionFactory: TopicConnectionFactory = broker.createTopicConnectionFactory

  def createConnectionFactory: ConnectionFactory = broker.createConnectionFactory

  def publishMessage(msg: String): Unit = {
    val conn = createTopicConnectionFactory.createTopicConnection()
    val session = conn.createTopicSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE)
    val topic = session.createTopic(topicName)
    val publisher = session.createPublisher(topic)
    publisher.send(session.createTextMessage(msg))
    Try { publisher.close() }
    Try { session.close() }
    Try { conn.close() }
  }

  def toSeq: Seq[String] = mutableList.toList

  def toJavaList: java.util.List[String] = {
    java.util.Collections.unmodifiableList(toSeq.asJava)
  }

  override def toString(): String = {
    getClass.getSimpleName + s"[${topicName}]"
  }
}

object JmsTopic {
  def apply(): JmsTopic = {
    new JmsTopic(JmsBroker())
  }
}
