/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub.{AcknowledgeRequest, PubSubConfig, PublishRequest, ReceivedMessage}
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.{GooglePubSub => GPubSub}
import akka.stream.javadsl.{Flow, Sink, Source}

import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._

object GooglePubSub {

  def publish(topic: String,
              config: PubSubConfig,
              parallelism: Int,
              actorSystem: ActorSystem,
              materializer: Materializer): Flow[PublishRequest, java.util.List[String], NotUsed] =
    GPubSub
      .publish(topic = topic, config = config, parallelism = parallelism)(actorSystem, materializer)
      .map(_.asJava)
      .asJava

  def subscribe(subscription: String,
                config: PubSubConfig,
                actorSystem: ActorSystem): Source[ReceivedMessage, NotUsed] =
    GPubSub
      .subscribe(
        subscription = subscription,
        config = config
      )(actorSystem)
      .asJava

  def acknowledge(subscription: String,
                  config: PubSubConfig,
                  parallelism: Int,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    GPubSub
      .acknowledge(subscription = subscription, config = config, parallelism = parallelism)(actorSystem, materializer)
      .mapMaterializedValue(_.toJava)
      .asJava
}
