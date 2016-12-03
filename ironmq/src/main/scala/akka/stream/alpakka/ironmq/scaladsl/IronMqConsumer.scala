/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.scaladsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.scaladsl._

object IronMqConsumer {

  def atMostOneConsumerSource(queueName: Queue.Name, clientProvider: () => IronMqClient): Source[Message, NotUsed] =
    Source.fromGraph(new IronMqSourceStage(queueName, clientProvider))

  def atMostOneConsumerSource(queueName: Queue.Name, client: IronMqClient): Source[Message, NotUsed] =
    atMostOneConsumerSource(queueName, () => client)

}
