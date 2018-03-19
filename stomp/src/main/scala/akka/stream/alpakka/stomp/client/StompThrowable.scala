/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import io.vertx.ext.stomp.{Frame, StompClientConnection}

sealed trait StompThrowable extends Exception

case class StompProtocolError(frame: Frame) extends StompThrowable {
  override def toString: String = super.toString + frame.toString
}

case class StompClientConnectionDropped(clientConnection: StompClientConnection) extends StompThrowable {
  override def toString: String = super.toString + clientConnection.toString
}
