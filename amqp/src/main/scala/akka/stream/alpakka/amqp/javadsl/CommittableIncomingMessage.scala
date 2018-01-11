/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp.IncomingMessage

trait CommittableIncomingMessage {
  val message: IncomingMessage
  def ack(multiple: Boolean = false): CompletionStage[Done]
  def nack(multiple: Boolean = false, requeue: Boolean = true): CompletionStage[Done]
}
