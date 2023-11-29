/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package jakartajmstestkit

import java.net.URI
import java.util.UUID

import javax.naming.Context
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.{BrokerFactory, BrokerService}
import org.apache.activemq.jndi.ActiveMQInitialContextFactory

import scala.util.Try

/**
 * This testkit was copied from https://github.com/sullis/jms-testkit with modifications
 * to support Jakarta Messaging. Replacing `javax.jms` with `jakarta.jms`.
 * jms-testkit is licensed under the Apache License, Version 2.0.
 */
class JmsBroker(val service: BrokerService) {
  checkState()

  def isStarted: Boolean = service.isStarted
  def isStopped: Boolean = service.isStopped

  def brokerUri: String = service.getDefaultSocketURIString

  def closeClientConnections(): Unit = {
    for (clientConn <- service.getBroker.getClients) {
      Try { clientConn.stop() }
    }
  }

  def clientConnectionCount: Int = service.getBroker.getClients.size

  def createQueueConnectionFactory: jakarta.jms.QueueConnectionFactory = {
    checkState()
    new ActiveMQConnectionFactory(service.getDefaultSocketURIString)
  }

  def createTopicConnectionFactory: jakarta.jms.TopicConnectionFactory = {
    checkState()
    new ActiveMQConnectionFactory(service.getDefaultSocketURIString)
  }

  def createConnectionFactory: jakarta.jms.ConnectionFactory = {
    checkState()
    new ActiveMQConnectionFactory(service.getDefaultSocketURIString)
  }

  def createJndiEnvironment: java.util.Hashtable[String, String] = {
    import scala.collection.JavaConverters._
    val env = new java.util.Hashtable[String, String]()
    env.put(Context.PROVIDER_URL, brokerUri)
    env.put(Context.INITIAL_CONTEXT_FACTORY, classOf[ActiveMQInitialContextFactory].getName)
    val destinations = service.getBroker.getDurableDestinations.asScala
    for (dest <- destinations) {
      val name = dest.getPhysicalName
      if (dest.isQueue) {
        env.put("queue." + name, name)
      } else if (dest.isTopic) {
        env.put("topic." + name, name)
      }
    }
    env
  }

  def createJndiContext: javax.naming.Context = {
    val factory = new ActiveMQInitialContextFactory
    factory.getInitialContext(createJndiEnvironment)
  }

  def start(force: Boolean = true): Unit = service.start(force)

  def restart(): Unit = {
    stop()
    start(force = true)
  }

  def stop(): Unit = service.stop()

  override def toString(): String = {
    getClass.getSimpleName + s"[${brokerUri}]"
  }

  private def checkState(): Unit = {
    if (service.isStopped) {
      throw new IllegalStateException("Broker is stopped")
    } else if (service.isStopping) {
      throw new IllegalStateException("Broker is stopping")
    }
  }
}

object JmsBroker {
  def apply(): JmsBroker = {
    val inMemoryBrokerName = "brokerName-" + UUID.randomUUID.toString
    val transportUri = s"vm://${inMemoryBrokerName}?create=false"
    val brokerConfigUri = new URI(s"broker:(${transportUri})/${inMemoryBrokerName}?persistent=false&useJmx=false")

    val brokerService = BrokerFactory.createBroker(brokerConfigUri)
    brokerService.setPersistent(false)
    brokerService.setUseJmx(false)
    brokerService.setStartAsync(false)
    brokerService.start()
    brokerService.waitUntilStarted()
    new JmsBroker(brokerService)
  }
}
