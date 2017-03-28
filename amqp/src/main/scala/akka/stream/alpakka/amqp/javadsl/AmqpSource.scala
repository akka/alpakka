/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{AmqpSourceSettings, AmqpSourceStage, IncomingMessage}
import akka.stream.javadsl.Source

object AmqpSource {

  /**
   * Java API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def create(settings: AmqpSourceSettings, bufferSize: Int): Source[IncomingMessage, NotUsed] =
    Source.fromGraph(new AmqpSourceStage(settings, bufferSize))

}
