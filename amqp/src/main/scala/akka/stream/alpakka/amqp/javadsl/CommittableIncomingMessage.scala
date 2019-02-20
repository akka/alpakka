/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.ApiMayChange
import akka.stream.alpakka.amqp.IncomingMessage

@ApiMayChange // https://github.com/akka/alpakka/issues/1513
trait CommittableIncomingMessage {
  val message: IncomingMessage
  def ack(multiple: Boolean = false): CompletionStage[Done]
  def nack(multiple: Boolean = false, requeue: Boolean = true): CompletionStage[Done]
}
