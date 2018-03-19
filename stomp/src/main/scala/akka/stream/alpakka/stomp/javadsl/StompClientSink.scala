/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.stomp.client.{ConnectorSettings, SendingFrame}

import scala.compat.java8.FutureConverters._

object StompClientSink {

  /**
   * Java API: Creates[[StompClientSink]] that accepts [[SendingFrame]] elements, and deliver them to a stomp server.
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Sink completes either normally or because of a stomp server failure.
   *
   */
  def create(settings: ConnectorSettings): akka.stream.javadsl.Sink[SendingFrame, CompletionStage[Done]] =
    akka.stream.alpakka.stomp.scaladsl.StompClientSink(settings).mapMaterializedValue(f => f.toJava).asJava

}
