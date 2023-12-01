/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.KillSwitch
import akka.stream.scaladsl.Source

/**
 * Internal API.
 */
@InternalApi private[jakartajms] trait JmsProducerMatValue {
  def connected: Source[InternalConnectionState, NotUsed]
}

/**
 * Internal API.
 */
@InternalApi private[jakartajms] trait JmsConsumerMatValue extends KillSwitch with JmsProducerMatValue
