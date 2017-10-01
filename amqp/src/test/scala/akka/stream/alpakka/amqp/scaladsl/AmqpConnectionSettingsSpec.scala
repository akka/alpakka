/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.stream.alpakka.amqp._

class AmqpConnectionSettingsSpec extends AmqpSpec {
  "The AMQP Connectors Settings" should {
    "create a new connection per invocation of DefaultAmqpConnection" in {
      val connection1 = DefaultAmqpConnection.getConnection
      val connection2 = DefaultAmqpConnection.getConnection
      connection1 should not equal (connection2)
      connection1.close
      connection2.close
    }

    "create a new connection per invocation of LocalAmqpConnection" in {
      val connectionSettings = LocalAmqpConnection()
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should not equal (connection2)
      connection1.close
      connection2.close
    }

    "create a new connection per invocation of AmqpConnectionUri" in {
      val connectionSettings = AmqpConnectionUri("amqp://localhost:5672")
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should not equal (connection2)
      connection1.close
      connection2.close
    }

    "create a new connection per invocation of AmqpConnectionDetails" in {
      val connectionSettings = AmqpConnectionDetails(List(("localhost", 5672)))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should not equal (connection2)
      connection1.close
      connection2.close
    }
  }

  "The AMQP Reusable Connectors Settings" should {
    "reuse the same connection from LocalAmqpConnection" in {
      val connectionSettings = ReusableAmqpConnectionSettings(LocalAmqpConnection())
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri" in {
      val connectionSettings = ReusableAmqpConnectionSettings(AmqpConnectionUri("amqp://localhost:5672"))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails" in {
      val connectionSettings = ReusableAmqpConnectionSettings(AmqpConnectionDetails(List(("localhost", 5672))))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }
  }

  "The AMQP Reusable Connectors Settings with automatic release" should {
    "reuse the same connection from LocalAmqpConnection and release it when the last client disconnects" in {
      val connectionSettings = ReusableAmqpConnectionSettingsWithAutomaticRelease(LocalAmqpConnection())
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri and release it when the last client disconnects" in {
      val connectionSettings =
        ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionUri("amqp://localhost:5672"))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails and release it when the last client disconnects" in {
      val connectionSettings =
        ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionDetails(List(("localhost", 5672))))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      connectionSettings.releaseConnection()
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from LocalAmqpConnection and release it if forced" in {
      val connectionSettings = ReusableAmqpConnectionSettingsWithAutomaticRelease(LocalAmqpConnection())
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection()
      connectionSettings.releaseConnection(force = true)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri and release it if forced" in {
      val connectionSettings =
        ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionUri("amqp://localhost:5672"))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection()
      connectionSettings.releaseConnection(force = true)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails and release it if forced" in {
      val connectionSettings =
        ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionDetails(List(("localhost", 5672))))
      val connection1 = connectionSettings.getConnection
      val connection2 = connectionSettings.getConnection
      connection1 should equal(connection2)
      connectionSettings.releaseConnection(force = true)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }
  }
}
