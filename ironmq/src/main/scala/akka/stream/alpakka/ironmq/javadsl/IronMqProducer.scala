/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import akka.NotUsed
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.alpakka.ironmq.scaladsl.{IronMqProducer => ScalaIronMqProducer}

object IronMqProducer {

  def producerFlow(queueName: String, clientProvider: () => IronMqClient): Flow[PushMessage, Message.Id, NotUsed] =
    ScalaIronMqProducer.producerFlow(Queue.Name(queueName), clientProvider).asJava

  def producerFlow(queueName: String, client: IronMqClient): Flow[PushMessage, Message.Id, NotUsed] =
    ScalaIronMqProducer.producerFlow(Queue.Name(queueName), client).asJava

  def producerSink(queueName: String, clientProvider: () => IronMqClient): Sink[PushMessage, NotUsed] =
    ScalaIronMqProducer.producerSink(Queue.Name(queueName), clientProvider).asJava

  def producerSink(queueName: String, client: IronMqClient): Sink[PushMessage, NotUsed] =
    ScalaIronMqProducer.producerSink(Queue.Name(queueName), client).asJava

}
