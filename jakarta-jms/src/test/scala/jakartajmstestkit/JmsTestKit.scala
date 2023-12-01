/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package jakartajmstestkit

/**
 * This testkit was copied from https://github.com/sullis/jms-testkit with modifications
 * to support Jakarta Messaging. Replacing `javax.jms` with `jakarta.jms`.
 * ActiveMQ replaced with Artemis EmbeddedActiveMQ.
 * jms-testkit is licensed under the Apache License, Version 2.0.
 */
trait JmsTestKit {
  def withBroker()(test: JmsBroker => Unit): Unit = {
    val broker = JmsBroker()
    try {
      test(broker)
      Thread.sleep(500)
    } finally {
      broker.stop()
    }
  }

  def withConnectionFactory()(test: jakarta.jms.ConnectionFactory => Unit): Unit = {
    withBroker() { broker =>
      test(broker.createConnectionFactory)
    }
  }

  def withQueue()(test: JmsQueue => Unit): Unit = {
    val queue = JmsQueue()
    try {
      test(queue)
      Thread.sleep(500)
    } finally {
      queue.broker.stop()
    }
  }

  def withTopic()(test: JmsTopic => Unit): Unit = {
    val topic = JmsTopic()
    try {
      test(topic)
      Thread.sleep(500)
    } finally {
      topic.broker.stop()
    }
  }

}
