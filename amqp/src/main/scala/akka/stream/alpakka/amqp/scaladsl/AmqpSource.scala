/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source}
import akka.util.ByteString

object AmqpSource {

  /**
   * Scala API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def apply(settings: AmqpSourceSettings, bufferSize: Int): Source[IncomingMessage[ByteString], NotUsed] =
    Source.fromGraph(new AmqpSourceStage(settings, bufferSize))

  /**
   * Scala API: Creates an [[AmqpSource]] with given settings and buffer size. You must manually ack the messages
   * with the given actor
   */
  def withoutAutoAck[Out](settings: AmqpSourceSettings,
                          bufferSize: Int,
                          flow: Flow[IncomingMessage[ByteString], Out, _]): Source[IncomingMessage[Out], NotUsed] =
    withoutAutoAck(settings, bufferSize).flatMapMerge(bufferSize, { m =>
      Source
        .single(m.message)
        .via(flow)
        .map { a =>
          m.ack().copy(bytes = a)
        }
        .mapError {
          case e =>
            m.nack(true)
            e
        }
    })

  /**
   * Scala API: Creates an [[AmqpSource]] with given settings and buffer size. You must manually ack the messages
   * with the given actor
   */
  def withoutAutoAck(settings: AmqpSourceSettings,
                     bufferSize: Int): Source[UnackedIncomingMessage[ByteString], NotUsed] =
    Source.fromGraph(new AmqpSourceWithoutAckStage(settings, bufferSize))
}
