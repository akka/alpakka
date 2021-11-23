/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms._
import jmstestkit.JmsBroker

import scala.util.Random

/**
 * Creates a single server and connection factory which is shared for all tests.
 */
abstract class JmsSharedServerSpec extends JmsSpec {
  private var jmsBroker: JmsBroker = _
  private var connectionFactory: ConnectionFactory = _

  override def beforeAll(): Unit = {
    jmsBroker = JmsBroker()
    connectionFactory = jmsBroker.createConnectionFactory
    Thread.sleep(500)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (jmsBroker != null && jmsBroker.isStarted) {
      jmsBroker.stop()
    }
  }

  protected def isQueueEmpty(queueName: String): Boolean = jmsBroker.service.checkQueueSize(queueName)

  override def withConnectionFactory()(test: ConnectionFactory => Unit): Unit = {
    test(connectionFactory)
  }

  def createName(prefix: String) = prefix + Random.nextInt().toString

}
