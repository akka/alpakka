/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.stomp.client.{ConnectorSettings, SendingFrame}

import scala.compat.java8.FutureConverters._

object StompClientSource {

  /**
   * Java API: Create a Source [[StompClientSource]] that receives
   * message from a stomp server. It listens to message at the `destination` described in settings.topic. It handles backpressure.
   */
  def create(settings: ConnectorSettings): akka.stream.javadsl.Source[SendingFrame, CompletionStage[Done]] =
    akka.stream.alpakka.stomp.scaladsl.StompClientSource(settings).mapMaterializedValue(f => f.toJava).asJava
}
