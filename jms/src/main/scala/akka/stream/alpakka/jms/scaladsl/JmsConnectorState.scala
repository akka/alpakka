/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl
import akka.NotUsed
import akka.stream.KillSwitch
import akka.stream.scaladsl.Source

trait JmsProducerStatus {

  /**
   * source that provides connector status change information.
   * Only the most recent connector state is buffered if the source is not consumed.
   */
  def connection: Source[JmsConnectorState, NotUsed]

}

trait JmsConsumerControl extends KillSwitch {

  /**
   * source that provides connector status change information.
   * Only the most recent connector state is buffered if the source is not consumed.
   */
  def connection: Source[JmsConnectorState, NotUsed]

}

sealed trait JmsConnectorState

object JmsConnectorState {
  case object Disconnected extends JmsConnectorState
  case class Connecting(attempt: Int) extends JmsConnectorState
  case object Connected extends JmsConnectorState
  case object Stopping extends JmsConnectorState
}
