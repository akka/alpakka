/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp.{AmqpSinkSettings, OutgoingMessage}
import akka.stream.alpakka.amqp.impl.AmqpPublishConfirmFlowStage
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

object AmqpPublishFlow {
  def withConfirms[O](settings: AmqpSinkSettings,
                      confirmTimeout: Long = 1000): Flow[(OutgoingMessage, O), O, Future[Done]] =
    Flow.fromGraph(new AmqpPublishConfirmFlowStage[O](settings, confirmTimeout))
}
