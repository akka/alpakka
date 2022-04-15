/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl;

import akka.stream.alpakka.amqp.RabbitMqFixedPortContainer;
import akka.stream.alpakka.amqp.RabbitMQTest$;

// See https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/
public class RabbitMQJUnitTest {
  public static final RabbitMqFixedPortContainer CONTAINER;

  static {
    CONTAINER = RabbitMQTest$.MODULE$.rabbitMQFixedPortContainer();
    CONTAINER.start();
  }
}
