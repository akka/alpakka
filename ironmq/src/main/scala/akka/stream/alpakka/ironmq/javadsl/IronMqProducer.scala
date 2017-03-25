/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.alpakka.ironmq.scaladsl.{IronMqProducer => ScalaIronMqProducer}

import scala.compat.java8.FutureConverters

object IronMqProducer {

  import FutureConverters._

  def producerFlow(queueName: String, settings: IronMqSettings): Flow[PushMessage, Message.Id, NotUsed] =
    ScalaIronMqProducer.producerFlow(Queue.Name(queueName), settings).asJava

  def producerSink(queueName: String, settings: IronMqSettings): Sink[PushMessage, CompletionStage[Done]] =
    ScalaIronMqProducer.producerSink(Queue.Name(queueName), settings).mapMaterializedValue(_.toJava).asJava

  def atLeastOnceProducerFlow(queueName: String,
                              settings: IronMqSettings): Flow[(PushMessage, Committable), Message.Id, NotUsed] =
    ScalaIronMqProducer.atLeastOnceProducerFlow(Queue.Name(queueName), settings).asJava

  def atLeastOnceProducerSink(queueName: String, settings: IronMqSettings): Sink[(PushMessage, Committable), NotUsed] =
    ScalaIronMqProducer.atLeastOnceProducerSink(Queue.Name(queueName), settings).asJava

  def atLeastOnceProducerFlow[ToCommit, CommitResult, CommitMat](queueName: String,
                                                                 settings: IronMqSettings,
                                                                 commitFlow: Flow[ToCommit, CommitResult, CommitMat])
    : Flow[(PushMessage, ToCommit), (Message.Id, CommitResult), CommitMat] =
    ScalaIronMqProducer.atLeastOnceProducerFlow(Queue.Name(queueName), settings, commitFlow.asScala).asJava

  def atLeastOnceProducerSink[ToCommit, CommitResult, CommitMat](
      queueName: String,
      settings: IronMqSettings,
      commitFlow: Flow[ToCommit, CommitResult, CommitMat]): Sink[(PushMessage, ToCommit), CommitMat] =
    ScalaIronMqProducer.atLeastOnceProducerSink(Queue.Name(queueName), settings, commitFlow.asScala).asJava

}
