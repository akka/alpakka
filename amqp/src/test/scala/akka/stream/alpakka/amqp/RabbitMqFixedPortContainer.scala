/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import com.dimafeng.testcontainers.FixedHostPortGenericContainer

// The FixedHostPortGenericContainer is needed to test LocalAmqpConnection
class RabbitMqFixedPortContainer
    extends FixedHostPortGenericContainer(
      "rabbitmq:3",
      exposedPorts = List(5672),
      exposedHostPort = 5672,
      exposedContainerPort = 5672
    )
