/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.alpakka.ironmq.scaladsl.{IronMqConsumer => ScalaIronMqConsumer}
import akka.stream.javadsl._

object IronMqConsumer {

  def atMostOnceConsumerSource(queueName: String, settings: IronMqSettings): Source[Message, NotUsed] =
    ScalaIronMqConsumer.atMostOnceConsumerSource(Queue.Name(queueName), settings).asJava

  def atLeastOnceConsumerSource(queueName: String, settings: IronMqSettings): Source[CommittableMessage, NotUsed] =
    ScalaIronMqConsumer.atLeastOnceConsumerSource(Queue.Name(queueName), settings).map(_.asJava).asJava

}
