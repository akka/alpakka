/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.javadsl

import java.util.concurrent.CompletionStage

import akka.japi.Pair
import akka.{Done, NotUsed}
import akka.stream.alpakka.ironmq._
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.scaladsl.{Keep, Flow => ScalaFlow}
import akka.stream.alpakka.ironmq.scaladsl.{IronMqProducer => ScalaIronMqProducer}

import scala.compat.java8.FutureConverters

object IronMqProducer {

  import FutureConverters._

  def producerFlow(queueName: String, settings: IronMqSettings): Flow[PushMessage, String, NotUsed] =
    ScalaIronMqProducer
      .producerFlow(queueName, settings)
      .asJava
      // To make Message.Id a String
      .asInstanceOf[Flow[PushMessage, String, NotUsed]]

  def producerSink(queueName: String, settings: IronMqSettings): Sink[PushMessage, CompletionStage[Done]] =
    ScalaIronMqProducer.producerSink(queueName, settings).mapMaterializedValue(_.toJava).asJava

  def atLeastOnceProducerFlow[C1 <: Committable](
      queueName: String,
      settings: IronMqSettings
  ): Flow[CommittablePushMessage[C1], String, NotUsed] =
    ScalaFlow[CommittablePushMessage[C1]]
      .map { cm =>
        cm.message -> cm.toCommit.asScala
      }
      .via(ScalaIronMqProducer.atLeastOnceProducerFlow(queueName, settings))
      .asJava
      // To make Message.Id a String
      .asInstanceOf[Flow[CommittablePushMessage[C1], String, NotUsed]]

  def atLeastOnceProducerSink[C1 <: Committable](queueName: String,
                                                 settings: IronMqSettings): Sink[CommittablePushMessage[C1], NotUsed] =
    ScalaFlow[CommittablePushMessage[C1]]
      .map { cm =>
        cm.message -> cm.toCommit.asScala
      }
      .to(ScalaIronMqProducer.atLeastOnceProducerSink(queueName, settings))
      .asJava

  def atLeastOnceProducerFlow[ToCommit, CommitResult, CommitMat](
      queueName: String,
      settings: IronMqSettings,
      commitFlow: Flow[ToCommit, CommitResult, CommitMat]
  ): Flow[CommittablePushMessage[ToCommit], Pair[String, CommitResult], CommitMat] =
    ScalaFlow[CommittablePushMessage[ToCommit]]
      .map { cm =>
        cm.message -> cm.toCommit
      }
      .viaMat(ScalaIronMqProducer.atLeastOnceProducerFlow(queueName, settings, commitFlow.asScala))(
        Keep.right
      )
      .map(p => Pair(p._1.asInstanceOf[String], p._2))
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
      .toMat(ScalaIronMqProducer.atLeastOnceProducerSink(queueName, settings, commitFlow.asScala))(
        Keep.right
      )
      .asJava

}

case class CommittablePushMessage[ToCommit](message: PushMessage, toCommit: ToCommit)
