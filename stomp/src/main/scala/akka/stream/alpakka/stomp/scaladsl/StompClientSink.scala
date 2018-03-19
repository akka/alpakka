/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.scaladsl

import akka.Done
import akka.stream.alpakka.stomp.client.{ConnectorSettings, SendingFrame, SinkStage}
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

object StompClientSink {

  /**
   * Scala API: Connects to a STOMP server upon materialization and sends incoming messages to the server.
   * Each materialized sink will create one connection to the broker. This stage sends messages to the destination
   * named in the settings options, if present, instead of the one written in the incoming message to the Sink.
   *
   * This stage materializes to a Future[Done], which can be used to know when the Sink completes, either normally
   * or because of a stomp failure.
   */
  def apply(settings: ConnectorSettings): Sink[SendingFrame, Future[Done]] = Sink.fromGraph(new SinkStage(settings))
}
