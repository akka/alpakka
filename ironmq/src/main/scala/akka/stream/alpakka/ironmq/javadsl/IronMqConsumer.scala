/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl._
import akka.stream.alpakka.ironmq.scaladsl.{IronMqConsumer => ScalaIronMqConsumer}

object IronMqConsumer {

  def atMostOneConsumerSource(queueName: Queue.Name, clientProvider: () => IronMqClient): Source[Message, NotUsed] =
    ScalaIronMqConsumer.atMostOneConsumerSource(queueName, clientProvider).asJava

  def atMostOneConsumerSource(queueName: Queue.Name, client: IronMqClient): Source[Message, NotUsed] =
    atMostOneConsumerSource(queueName, () => client)

}
