/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{AmqpSourceSettings, AmqpSourceStage, IncomingMessage}

import akka.stream.scaladsl.Source

object AmqpSource {

  /**
   * Scala API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def apply(settings: AmqpSourceSettings, bufferSize: Int): Source[IncomingMessage, NotUsed] =
    Source.fromGraph(new AmqpSourceStage(settings, bufferSize))

}
