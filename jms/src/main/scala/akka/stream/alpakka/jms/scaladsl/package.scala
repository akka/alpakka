/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import akka.{Done, NotUsed}
import akka.annotation.InternalApi
import akka.stream.alpakka.jms.impl.InternalConnectionState
import akka.stream.scaladsl.Source

import scala.util.{Failure, Success}

package object scaladsl {
  @InternalApi private[scaladsl] def transformConnectorState(source: Source[InternalConnectionState, NotUsed]) = {
    import InternalConnectionState._
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
}
