/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.javadsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.alpakka.ironmq.scaladsl.{IronMqConsumer => ScalaIronMqConsumer}
import akka.stream.javadsl._

object IronMqConsumer {

  def atMostOnceSource(queueName: String, settings: IronMqSettings): Source[Message, NotUsed] =
    ScalaIronMqConsumer.atMostOnceSource(queueName, settings).asJava

  def atLeastOnceSource(queueName: String, settings: IronMqSettings): Source[CommittableMessage, NotUsed] =
    ScalaIronMqConsumer.atLeastOnceSource(queueName, settings).map(_.asJava).asJava

}
