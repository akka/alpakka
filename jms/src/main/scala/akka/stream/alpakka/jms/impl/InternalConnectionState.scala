/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.Done
import akka.annotation.InternalApi
import javax.jms

import scala.concurrent.Future
import scala.util.Try

/**
 * Internal API.
 */
@InternalApi
private[jms] trait InternalConnectionState

/**
 * Internal API.
 */
@InternalApi
private[jms] object InternalConnectionState {
  case object JmsConnectorDisconnected extends InternalConnectionState
  case class JmsConnectorInitializing(connection: Future[jms.Connection],
                                      attempt: Int,
                                      backoffMaxed: Boolean,
                                      sessions: Int)
      extends InternalConnectionState
  case class JmsConnectorConnected(connection: jms.Connection) extends InternalConnectionState
  case class JmsConnectorStopping(completion: Try[Done]) extends InternalConnectionState
  case class JmsConnectorStopped(completion: Try[Done]) extends InternalConnectionState
}
