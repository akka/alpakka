/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package jakartajmstestkit

import org.apache.activemq.artemis.api.core.management.QueueControl
import org.apache.activemq.artemis.api.core.management.ResourceNames
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory
import org.apache.activemq.artemis.jms.client.ActiveMQTopicConnectionFactory

/**
 * This testkit was copied from https://github.com/sullis/jms-testkit with modifications
 * to support Jakarta Messaging. Replacing `javax.jms` with `jakarta.jms`.
 * ActiveMQ replaced with Artemis EmbeddedActiveMQ.
 * jms-testkit is licensed under the Apache License, Version 2.0.
 */
class JmsBroker(val broker: EmbeddedActiveMQ) {

  def isStarted: Boolean = broker.getActiveMQServer.isStarted

  def brokerUri: String = "vm://0"

  def createQueueConnectionFactory: jakarta.jms.QueueConnectionFactory = {
    new ActiveMQQueueConnectionFactory(brokerUri)
  }

  def createTopicConnectionFactory: jakarta.jms.TopicConnectionFactory = {
    new ActiveMQTopicConnectionFactory(brokerUri)
  }

  def createConnectionFactory: jakarta.jms.ConnectionFactory = {
    new ActiveMQConnectionFactory(brokerUri)
  }

  def start(): Unit = broker.start()

  def restart(): Unit = {
    stop()
    start()
  }

  def stop(): Unit = broker.getActiveMQServer.stop()

  def getQueueSize(queueName: String): Long = {
    val queueControl =
      broker.getActiveMQServer.getManagementService
        .getResource(ResourceNames.QUEUE + queueName)
        .asInstanceOf[QueueControl]
    queueControl.getMessageCount
  }

  override def toString(): String = {
    getClass.getSimpleName + s"[$brokerUri]"
  }

}

object JmsBroker {
  def apply(): JmsBroker = {
    val config = new ConfigurationImpl
    config.addAcceptorConfiguration("in-vm", "vm://0")
    config.setSecurityEnabled(false)
    config.setPersistenceEnabled(false)
    val broker = new EmbeddedActiveMQ
    broker.setConfiguration(config)
    broker.start()

    new JmsBroker(broker)
  }
}
