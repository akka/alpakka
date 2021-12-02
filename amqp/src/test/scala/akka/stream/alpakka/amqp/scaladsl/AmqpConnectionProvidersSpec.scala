/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import java.net.ConnectException

import akka.stream.alpakka.amqp._
import com.rabbitmq.client.ConnectionFactory

class AmqpConnectionProvidersSpec extends AmqpSpec {
  "The AMQP Default Connection Providers" should {
    "create a new connection per invocation of LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider("localhost", 5672)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider("localhost", 5672)
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider =
        AmqpConnectionFactoryConnectionProvider(connectionFactory).withHostAndPort("localhost", 5672)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider =
        AmqpConnectionFactoryConnectionProvider(connectionFactory).withHostAndPort("localhost", 5672)
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }
  }

  "The AMQP Reusable Connection Provider with automatic release" should {
    "reuse the same connection from LocalAmqpConnection and release it when the last client disconnects" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri and release it when the last client disconnects" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails and release it when the last client disconnects" in {
      val connectionProvider = AmqpDetailsConnectionProvider("localhost", 5672)
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionFactory and release it when the last client disconnects" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider =
        AmqpConnectionFactoryConnectionProvider(connectionFactory).withHostAndPort("localhost", 5672)
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }
  }

  "The AMQP Reusable Connection Provider without automatic release" should {
    "reuse the same connection from LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider).withAutomaticRelease(false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider).withAutomaticRelease(false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider("localhost", 5672)
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider).withAutomaticRelease(false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider =
        AmqpConnectionFactoryConnectionProvider(connectionFactory).withHostAndPort("localhost", 5672)
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider).withAutomaticRelease(false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "not leave the provider in an invalid state if getting the connection fails" in {
      val connectionProvider = AmqpDetailsConnectionProvider("localhost", 5673)
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider).withAutomaticRelease(false)
      try reusableConnectionProvider.get
      catch { case e: Throwable => e shouldBe an[ConnectException] }
      try reusableConnectionProvider.get
      catch { case e: Throwable => e shouldBe an[ConnectException] }
    }
  }
}
