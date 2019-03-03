/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
