/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import akka.{Done, NotUsed}
import akka.annotation.InternalApi
import akka.stream.alpakka.jms.impl.JmsConnector.{
  JmsConnectorConnected,
  JmsConnectorDisconnected,
  JmsConnectorInitializing,
  JmsConnectorStopped,
  JmsConnectorStopping,
  JmsConnectorState => InternalJmsConnectorState
}
import akka.stream.scaladsl.Source

import scala.util.{Failure, Success}

package object scaladsl {
  @InternalApi private[scaladsl] def transformConnectorState(source: Source[InternalJmsConnectorState, NotUsed]) =
    source.map {
      case JmsConnectorDisconnected => JmsConnectorState.Disconnected
      case _: JmsConnectorConnected => JmsConnectorState.Connected
      case i: JmsConnectorInitializing => JmsConnectorState.Connecting(i.attempt + 1)
      case JmsConnectorStopping(Success(Done)) => JmsConnectorState.Completing
      case JmsConnectorStopping(Failure(t)) => JmsConnectorState.Failing(t)
      case JmsConnectorStopped(Success(Done)) => JmsConnectorState.Completed
      case JmsConnectorStopped(Failure(t)) => JmsConnectorState.Failed(t)
    }
}
