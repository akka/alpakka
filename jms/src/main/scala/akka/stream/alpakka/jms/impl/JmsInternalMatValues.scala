/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.KillSwitch
import JmsConnector.JmsConnectorState
import akka.stream.scaladsl.Source

@InternalApi private[jms] trait JmsProducerMatValue {
  def connected: Source[JmsConnectorState, NotUsed]
}

@InternalApi private[jms] trait JmsConsumerMatValue extends KillSwitch with JmsProducerMatValue
