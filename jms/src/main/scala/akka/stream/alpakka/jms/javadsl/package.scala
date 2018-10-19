/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream
import akka.stream.alpakka.jms
import akka.stream.alpakka.jms.scaladsl.JmsConnectorState._
import akka.stream.javadsl.Source

package object javadsl {

  @InternalApi private[javadsl] def transformConnected(
      source: stream.scaladsl.Source[jms.scaladsl.JmsConnectorState, NotUsed]
  ): Source[JmsConnectorState, NotUsed] =
    source.map {
      case Disconnected => JmsConnectorState.Disconnected
      case Connected => JmsConnectorState.Connected
      case Connecting(_) => JmsConnectorState.Connecting
      case Completing => JmsConnectorState.Completing
      case Completed => JmsConnectorState.Completed
      case Failing(_) => JmsConnectorState.Failing
      case Failed(_) => JmsConnectorState.Failed
    }.asJava
}
