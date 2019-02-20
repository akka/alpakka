/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.KillSwitch
import akka.stream.scaladsl.Source

/**
 * Internal API.
 */
@InternalApi private[jms] trait JmsProducerMatValue {
  def connected: Source[InternalConnectionState, NotUsed]
}

/**
 * Internal API.
 */
@InternalApi private[jms] trait JmsConsumerMatValue extends KillSwitch with JmsProducerMatValue
