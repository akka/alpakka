/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import org.scalatest.{BeforeAndAfterAll, Suite}

// Ideally we would use the provided testcontainers-scala-scalatest
// ForAllTestContainer however we need to share a global singleton
// instance of RabbitMqFixedPortContainer with the JUnit tests.
trait RabbitMQTest extends BeforeAndAfterAll { this: Suite =>
  import RabbitMQTest._
  lazy val container: RabbitMqFixedPortContainer = rabbitMQFixedPortContainer

  override def beforeAll(): Unit =
    rabbitMQFixedPortContainer
}

object RabbitMQTest {
  lazy val credentials: AmqpCredentials = AmqpCredentials("user", "password")
  lazy val rabbitMQFixedPortContainer: RabbitMqFixedPortContainer = {
    val container = new RabbitMqFixedPortContainer
    container.start()
    container
  }
}
