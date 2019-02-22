/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.amqp.impl
import akka.stream.alpakka.amqp.{AmqpSourceSettings, ReadResult}
import akka.stream.scaladsl.Source

object AmqpSource {
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  /**
   * Scala API: Convenience for "at-most once delivery" semantics. Each message is acked to RabbitMQ
   * before it is emitted downstream.
   */
  def atMostOnceSource(settings: AmqpSourceSettings, bufferSize: Int): Source[ReadResult, NotUsed] =
    committableSource(settings, bufferSize)
      .mapAsync(1)(cm => cm.ack().map(_ => cm.message))

  /**
   * Scala API:
   * The `committableSource` makes it possible to commit (ack/nack) messages to RabbitMQ.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceSource]].
   *
   * Compared to auto-commit, this gives exact control over when a message is considered consumed.
   */
  def committableSource(settings: AmqpSourceSettings, bufferSize: Int): Source[CommittableReadResult, NotUsed] =
    Source.fromGraph(new impl.AmqpSourceStage(settings, bufferSize))

}
