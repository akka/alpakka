/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.scaladsl

import akka.Done
import akka.stream.alpakka.stomp.client.{ConnectorSettings, SendingFrame, SourceStage}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object StompClientSource {

  /**
   * Scala API: Upon materialization this source [[StompClientSource]] connects and subscribes to a topic (set in settings) published in a Stomp server. Each message may be Ack, and handles backpressure.
   */
  def apply(settings: ConnectorSettings): Source[SendingFrame, Future[Done]] =
    Source.fromGraph(new SourceStage(settings))

}
