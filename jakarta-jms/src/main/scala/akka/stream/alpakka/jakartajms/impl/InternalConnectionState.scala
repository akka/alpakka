/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.impl

import akka.Done
import akka.annotation.InternalApi
import jakarta.jms

import scala.concurrent.Future
import scala.util.Try

/**
 * Internal API.
 */
@InternalApi
private[jakartajms] trait InternalConnectionState

/**
 * Internal API.
 */
@InternalApi
private[jakartajms] object InternalConnectionState {
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
