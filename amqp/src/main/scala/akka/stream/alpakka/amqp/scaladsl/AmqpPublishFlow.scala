/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp.{AmqpSinkSettings, OutgoingMessage}
import akka.stream.alpakka.amqp.impl.AmqpPublishFlowStage
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString

import scala.concurrent.Future

object AmqpPublishFlow {
  def simple[O](settings: AmqpSinkSettings): Flow[(ByteString, O), O, Future[Done]] =
    Flow[(ByteString, O)]
      .map { case (s, passthrough) => (OutgoingMessage(s, false, false), passthrough) }
      .viaMat(apply[O](settings))(Keep.right)

  def apply[O](settings: AmqpSinkSettings): Flow[(OutgoingMessage, O), O, Future[Done]] =
    Flow.fromGraph(new AmqpPublishFlowStage[O](settings))
}
