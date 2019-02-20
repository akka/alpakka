/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.stream.alpakka.amqp.IncomingMessage

import scala.concurrent.Future

@ApiMayChange // https://github.com/akka/alpakka/issues/1513
trait CommittableIncomingMessage {
  val message: IncomingMessage
  def ack(multiple: Boolean = false): Future[Done]
  def nack(multiple: Boolean = false, requeue: Boolean = true): Future[Done]
}
