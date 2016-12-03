/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.scaladsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.scaladsl._

object IronMqProducer {

  def producerFlow(queueName: Queue.Name, clientProvider: () => IronMqClient): Flow[PushMessage, Message.Id, NotUsed] =
    Flow.fromGraph(new IronMqPushMessageStage(queueName, clientProvider)).mapAsync(3)(identity).mapConcat(_.ids)

  def producerFlow(queueName: Queue.Name, client: IronMqClient): Flow[PushMessage, Message.Id, NotUsed] =
    producerFlow(queueName, () => client)

  def producerSink(queueName: Queue.Name, clientProvider: () => IronMqClient): Sink[PushMessage, NotUsed] =
    producerFlow(queueName, clientProvider).to(Sink.ignore)

  def producerSink(queueName: Queue.Name, client: IronMqClient): Sink[PushMessage, NotUsed] =
    producerFlow(queueName, client).to(Sink.ignore)

}
