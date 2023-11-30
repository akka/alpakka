/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.scaladsl

import jakarta.jms.{Connection, ConnectionFactory}

import org.apache.activemq.artemis.jms.client.ActiveMQConnection

/**
 * a silly cached connection factory, not thread safe
 */
class CachedConnectionFactory(connFactory: ConnectionFactory) extends ConnectionFactory {

  var cachedConnection: ActiveMQConnection = null

  override def createConnection(): Connection = {
    if (cachedConnection == null) {
      cachedConnection = connFactory.createConnection().asInstanceOf[ActiveMQConnection]
    }
    cachedConnection
  }

  override def createConnection(s: String, s1: String): Connection = cachedConnection

  // added in JMS 2.0
  // see https://github.com/akka/alpakka/issues/1493
  def createContext(x$1: Int): jakarta.jms.JMSContext = ???
  def createContext(x$1: String, x$2: String, x$3: Int): jakarta.jms.JMSContext = ???
  def createContext(x$1: String, x$2: String): jakarta.jms.JMSContext = ???
  def createContext(): jakarta.jms.JMSContext = ???
}
