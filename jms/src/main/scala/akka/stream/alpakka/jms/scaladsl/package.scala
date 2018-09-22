/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.jms.JmsConnector.{
  JmsConnectorState => InternalJmsConnectorState,
  JmsConnectorConnected,
  JmsConnectorDisconnected,
  JmsConnectorInitializing,
  JmsConnectorStopping
}
import akka.stream.scaladsl.Source

package object scaladsl {
  @InternalApi private[scaladsl] def transformConnected(source: Source[InternalJmsConnectorState, NotUsed]) =
    source.map {
      case JmsConnectorDisconnected => JmsConnectorState.Disconnected
      case _: JmsConnectorConnected => JmsConnectorState.Connected
      case i: JmsConnectorInitializing => JmsConnectorState.Connecting(i.attempt + 1)
      case JmsConnectorStopping => JmsConnectorState.Stopping
    }
}
