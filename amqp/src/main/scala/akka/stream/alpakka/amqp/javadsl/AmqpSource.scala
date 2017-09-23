/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{AmqpSourceSettings, AmqpSourceStage, CommittableIncomingMessage, IncomingMessage}
import akka.stream.javadsl.Source

object AmqpSource {

  /**
   * Java API: Creates an [[AmqpSource]] with given settings and buffer size.
   */
  def create(settings: AmqpSourceSettings, bufferSize: Int): Source[IncomingMessage, NotUsed] =
    atMostOnceSource(settings, bufferSize)

  /**
   * Java API: Convenience for "at-most once delivery" semantics. Each message is acked to Kafka
   * before it is emitted downstream.
   */
  def atMostOnceSource(settings: AmqpSourceSettings, bufferSize: Int): Source[IncomingMessage, NotUsed] =
    committableSource(settings, bufferSize)
      .map(cm => {
        cm.ack()
        cm.message
      })

  /**
   * Java API:
   * The `committableSource` makes it possible to commit (ack/nack) messages to RabbitMQ.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceSource]].
   *
   * Compared to auto-commit, this gives exact control over when a message is considered consumed.
   */
  def committableSource(settings: AmqpSourceSettings, bufferSize: Int): Source[CommittableIncomingMessage, NotUsed] =
    Source.fromGraph(new AmqpSourceStage(settings, bufferSize))

}
