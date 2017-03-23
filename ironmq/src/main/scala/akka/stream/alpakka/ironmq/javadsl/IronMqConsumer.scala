/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl._
import akka.stream.alpakka.ironmq.scaladsl.{IronMqConsumer => ScalaIronMqConsumer}

object IronMqConsumer {

  def atMostOneConsumerSource(queueName: Queue.Name, settings: IronMqSettings): Source[Message, NotUsed] =
    ScalaIronMqConsumer.atMostOneConsumerSource(queueName, settings).asJava

  def atLeastOneConsumerSource(queueName: Queue.Name, settings: IronMqSettings): Source[CommittableMessage, NotUsed] =
    ScalaIronMqConsumer.atLeastOnceConsumerSource(queueName, settings).asJava

}
