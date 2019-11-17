/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.{GooglePubSub => GPubSub}
import akka.stream.alpakka.googlecloud.pubsub.{AcknowledgeRequest, PubSubConfig, PublishRequest, ReceivedMessage}
import akka.stream.javadsl.{Flow, FlowWithContext, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

object GooglePubSub {

  def publish(topic: String,
              config: PubSubConfig,
              parallelism: Int,
              actorSystem: ActorSystem,
              materializer: Materializer): Flow[PublishRequest, java.util.List[String], NotUsed] =
    GPubSub
      .publish(topic = topic, config = config, parallelism = parallelism)(actorSystem, materializer)
      .map(response => response.asJava)
      .asJava

  def publishWithContext[C](
      topic: String,
      config: PubSubConfig,
      parallelism: Int,
      actorSystem: ActorSystem,
      materializer: Materializer
  ): FlowWithContext[PublishRequest, C, java.util.List[String], C, NotUsed] =
    GPubSub
      .publishWithContext[C](topic = topic, config = config, parallelism = parallelism)(actorSystem, materializer)
      .map(response => response.asJava)
      .asJava

  def subscribe(subscription: String,
                config: PubSubConfig,
                actorSystem: ActorSystem,
                materializer: Materializer): Source[ReceivedMessage, Cancellable] =
    GPubSub
      .subscribe(subscription = subscription, config = config)(actorSystem, materializer)
      .asJava

  def acknowledge(subscription: String,
                  config: PubSubConfig,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    GPubSub
      .acknowledge(subscription = subscription, config = config)(actorSystem, materializer)
      .mapMaterializedValue(_.toJava)
      .asJava
}
