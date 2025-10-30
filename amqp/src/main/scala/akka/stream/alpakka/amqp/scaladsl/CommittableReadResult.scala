/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp.ReadResult

import scala.concurrent.Future

trait CommittableReadResult {
  val message: ReadResult
  def ack(multiple: Boolean = false): Future[Done]
  def nack(multiple: Boolean = false, requeue: Boolean = true): Future[Done]
}
