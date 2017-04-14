/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.scaladsl.{Keep, Flow => ScalaFlow}
import akka.stream.alpakka.ironmq.scaladsl.{IronMqProducer => ScalaIronMqProducer}

import scala.compat.java8.FutureConverters

object IronMqProducer {

  import FutureConverters._

  def producerFlow(queueName: String, settings: IronMqSettings): Flow[PushMessage, Message.Id, NotUsed] =
    ScalaIronMqProducer.producerFlow(Queue.Name(queueName), settings).asJava

  def producerSink(queueName: String, settings: IronMqSettings): Sink[PushMessage, CompletionStage[Done]] =
    ScalaIronMqProducer.producerSink(Queue.Name(queueName), settings).mapMaterializedValue(_.toJava).asJava

  def atLeastOnceProducerFlow[C1 <: Committable](
      queueName: String,
      settings: IronMqSettings
  ): Flow[CommittablePushMessage[C1], Message.Id, NotUsed] =
    ScalaFlow[CommittablePushMessage[C1]]
      .map { cm =>
        cm.message -> cm.toCommit.asScala
      }
      .via(ScalaIronMqProducer.atLeastOnceProducerFlow(Queue.Name(queueName), settings))
      .asJava

  def atLeastOnceProducerSink[C1 <: Committable](queueName: String,
                                                 settings: IronMqSettings): Sink[CommittablePushMessage[C1], NotUsed] =
    ScalaFlow[CommittablePushMessage[C1]]
      .map { cm =>
        cm.message -> cm.toCommit.asScala
      }
      .to(ScalaIronMqProducer.atLeastOnceProducerSink(Queue.Name(queueName), settings))
      .asJava

  def atLeastOnceProducerFlow[ToCommit, CommitResult, CommitMat](
      queueName: String,
      settings: IronMqSettings,
      commitFlow: Flow[ToCommit, CommitResult, CommitMat]
  ): Flow[CommittablePushMessage[ToCommit], (Message.Id, CommitResult), CommitMat] =
    ScalaFlow[CommittablePushMessage[ToCommit]]
      .map { cm =>
        cm.message -> cm.toCommit
      }
      .viaMat(ScalaIronMqProducer.atLeastOnceProducerFlow(Queue.Name(queueName), settings, commitFlow.asScala))(
        Keep.right
      )
      .asJava

  def atLeastOnceProducerSink[ToCommit, CommitResult, CommitMat](
      queueName: String,
      settings: IronMqSettings,
      commitFlow: Flow[ToCommit, CommitResult, CommitMat]
  ): Sink[CommittablePushMessage[ToCommit], CommitMat] =
    ScalaFlow[CommittablePushMessage[ToCommit]]
      .map { cm =>
        cm.message -> cm.toCommit
      }
      .toMat(ScalaIronMqProducer.atLeastOnceProducerSink(Queue.Name(queueName), settings, commitFlow.asScala))(
        Keep.right
      )
      .asJava

}

case class CommittablePushMessage[ToCommit](message: PushMessage, toCommit: ToCommit)
