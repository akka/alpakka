/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms
import akka.{Done, NotUsed}
import akka.annotation.InternalApi
import akka.stream.alpakka.jakartajms.impl.InternalConnectionState
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
      case other => throw new MatchError(other)
    }
  }
}
