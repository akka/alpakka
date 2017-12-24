/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.stream.alpakka.amqp._

class AmqpConnectionProvidersSpec extends AmqpSpec {
  "The AMQP Default Connection Provider" should {
    "create a new connection per invocation of DefaultAmqpConnection" in {
      val connectionProvider = DefaultAmqpConnectionProvider.create(DefaultAmqpConnection)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "create a new connection per invocation of LocalAmqpConnection" in {
      val connectionSettings = AmqpConnectionLocal()
      val connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "create a new connection per invocation of AmqpConnectionUri" in {
      val connectionSettings = AmqpConnectionUri("amqp://localhost:5672")
      val connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "create a new connection per invocation of AmqpConnectionDetails" in {
      val connectionSettings = AmqpConnectionDetails(List(("localhost", 5672)))
      val connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }
  }

  "The AMQP Reusable Connection Provider with automatic release" should {
    "reuse the same connection from LocalAmqpConnection and release it when the last client disconnects" in {
      val connectionSettings = AmqpConnectionLocal()
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri and release it when the last client disconnects" in {
      val connectionSettings = AmqpConnectionUri("amqp://localhost:5672")
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails and release it when the last client disconnects" in {
      val connectionSettings = AmqpConnectionDetails(List(("localhost", 5672)))
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }
  }

  "The AMQP Reusable Connection Provider without automatic release" should {
    "reuse the same connection from LocalAmqpConnection" in {
      val connectionSettings = AmqpConnectionLocal()
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings, automaticRelease = false)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri" in {
      val connectionSettings = AmqpConnectionUri("amqp://localhost:5672")
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings, automaticRelease = false)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails" in {
      val connectionSettings = AmqpConnectionDetails(List(("localhost", 5672)))
      val connectionProvider = ReusableAmqpConnectionProvider(connectionSettings, automaticRelease = false)
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should equal(connection2)
      connectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }
  }
}
