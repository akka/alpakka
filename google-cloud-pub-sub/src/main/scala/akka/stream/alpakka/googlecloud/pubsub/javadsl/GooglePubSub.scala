/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.Cancellable
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.{GooglePubSub => GPubSub}
import akka.stream.alpakka.googlecloud.pubsub.{AcknowledgeRequest, PubSubConfig, PublishRequest, ReceivedMessage}
import akka.stream.javadsl.{Flow, FlowWithContext, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Java DSL for Google Pub/Sub
 */
object GooglePubSub {

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids.
   */
  def publish(topic: String,
              config: PubSubConfig,
              parallelism: Int
  ): Flow[PublishRequest, java.util.List[String], NotUsed] =
    GPubSub
      .publish(topic, config, parallelism)
      .map(response => response.asJava)
      .asJava

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids and carries a context
   * through.
   */
  def publishWithContext[C](topic: String,
                            config: PubSubConfig,
                            parallelism: Int
  ): FlowWithContext[PublishRequest, C, java.util.List[String], C, NotUsed] =
    GPubSub
      .publishWithContext[C](topic, config, parallelism)
      .map(response => response.asJava)
      .asJava

  /**
   * Creates a source pulling messages from a subscription.
   */
  def subscribe(subscription: String, config: PubSubConfig): Source[ReceivedMessage, Cancellable] =
    GPubSub
      .subscribe(subscription, config)
      .asJava

  /**
   * Creates a flow pulling messages from a subscription.
   */
  def subscribeFlow(subscription: String, config: PubSubConfig): Flow[Done, ReceivedMessage, Future[NotUsed]] =
    GPubSub
      .subscribeFlow(subscription, config)
      .asJava

  /**
   * Creates a sink for acknowledging messages on a subscription.
   *
   * @deprecated Use `acknowledge` without `parallelism` param (since 2.0.0)
   */
  @Deprecated
  @deprecated("Use `acknowledge` without `parallelism` param", since = "2.0.0")
  def acknowledge(subscription: String,
                  config: PubSubConfig,
                  parallelism: Int
  ): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    acknowledge(subscription, config)

  /**
   * Creates a flow for acknowledging messages on a subscription.
   */
  def acknowledgeFlow(subscription: String, config: PubSubConfig): Flow[AcknowledgeRequest, Done, NotUsed] =
    GPubSub
      .acknowledgeFlow(subscription, config)
      .asJava

  /**
   * Creates a sink for acknowledging messages on a subscription.
   */
  def acknowledge(subscription: String, config: PubSubConfig): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    GPubSub
      .acknowledge(subscription, config)
      .mapMaterializedValue(_.toJava)
      .asJava
}
