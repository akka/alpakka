/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import akka.actor.Cancellable
import akka.stream.Attributes
import akka.stream.alpakka.google.GoogleAttributes
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.impl._
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Scala DSL for Google Pub/Sub
 */
object GooglePubSub extends GooglePubSub {
  private[pubsub] override val httpApi = PubSubApi
}

protected[pubsub] trait GooglePubSub {
  private[pubsub] def httpApi: PubSubApi

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids.
   * @param overrideHost publish message will be sent to specific host,
   *                     can be used to send message to specific regional endpoint,
   *                     which can be important when ordering is enabled
   */
  def publish(topic: String,
              config: PubSubConfig,
              overrideHost: String,
              parallelism: Int): Flow[PublishRequest, immutable.Seq[String], NotUsed] =
    internalPublish(topic, config, Some(overrideHost), parallelism)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids.
   * @param overrideHost publish message will be sent to specific host,
   *                     can be used to send message to specific regional endpoint,
   *                     which can be important when ordering is enabled
   */
  def publish(topic: String,
              config: PubSubConfig,
              overrideHost: String): Flow[PublishRequest, immutable.Seq[String], NotUsed] =
    internalPublish(topic, config, Some(overrideHost), parallelism = 1)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids.
   */
  def publish(topic: String,
              config: PubSubConfig,
              parallelism: Int = 1): Flow[PublishRequest, immutable.Seq[String], NotUsed] =
    internalPublish(topic, config, None, parallelism)

  private def internalPublish(topic: String,
                              config: PubSubConfig,
                              overrideHost: Option[String],
                              parallelism: Int): Flow[PublishRequest, immutable.Seq[String], NotUsed] =
    Flow[PublishRequest]
      .map((_, ()))
      .via(
        internalPublishWithContext[Unit](topic, config, overrideHost, parallelism).asFlow
      )
      .map(_._1)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids and carries a context
   * through.
   * @param overrideHost publish message will be sent to specific host,
   *                     can be used to send message to specific regional endpoint,
   *                     which can be important when ordering is enabled
   */
  def publishWithContext[C](
      topic: String,
      config: PubSubConfig,
      overrideHost: String,
      parallelism: Int
  ): FlowWithContext[PublishRequest, C, immutable.Seq[String], C, NotUsed] =
    internalPublishWithContext(topic, config, Some(overrideHost), parallelism)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids and carries a context
   * through.
   * @param overrideHost publish message will be sent to specific host,
   *                     can be used to send message to specific regional endpoint,
   *                     which can be important when ordering is enabled
   */
  def publishWithContext[C](
      topic: String,
      config: PubSubConfig,
      overrideHost: String
  ): FlowWithContext[PublishRequest, C, immutable.Seq[String], C, NotUsed] =
    internalPublishWithContext(topic, config, Some(overrideHost), parallelism = 1)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids and carries a context
   * through.
   */
  def publishWithContext[C](
      topic: String,
      config: PubSubConfig,
      parallelism: Int = 1
  ): FlowWithContext[PublishRequest, C, immutable.Seq[String], C, NotUsed] =
    internalPublishWithContext(topic, config, None, parallelism)

  private def internalPublishWithContext[C](
      topic: String,
      config: PubSubConfig,
      overrideHost: Option[String],
      parallelism: Int
  ): FlowWithContext[PublishRequest, C, immutable.Seq[String], C, NotUsed] =
    // some wrapping back and forth as FlowWithContext doesn't offer `setup`
    // https://github.com/akka/akka/issues/27883
    FlowWithContext
      .fromTuples(flow(config)(httpApi.publish[C](topic, parallelism, overrideHost).asFlow))
      .map(_.messageIds)

  /**
   * Creates a source pulling messages from a subscription.
   */
  def subscribe(subscription: String, config: PubSubConfig): Source[ReceivedMessage, Cancellable] = {
    Source
      .tick(0.seconds, 1.second, Done)
      .via(subscribeFlow(subscription, config))
  }

  /**
   * Creates a flow pulling messages from a subscription.
   */
  def subscribeFlow(subscription: String, config: PubSubConfig): Flow[Done, ReceivedMessage, Future[NotUsed]] = {
    flow(config)(httpApi.pull(subscription, config.pullReturnImmediately, config.pullMaxMessagesPerInternalBatch))
      .mapConcat(_.receivedMessages.getOrElse(Seq.empty[ReceivedMessage]).toIndexedSeq)
  }.mapMaterializedValue(_ => Future.successful(NotUsed))

  /**
   * Creates a flow for acknowledging messages on a subscription.
   */
  def acknowledgeFlow(subscription: String, config: PubSubConfig): Flow[AcknowledgeRequest, Done, NotUsed] =
    flow(config)(httpApi.acknowledge(subscription))

  /**
   * Creates a sink for acknowledging messages on a subscription.
   */
  def acknowledge(subscription: String, config: PubSubConfig): Sink[AcknowledgeRequest, Future[Done]] =
    acknowledgeFlow(subscription, config).toMat(Sink.ignore)(Keep.right)

  @nowarn("msg=deprecated")
  private def flow[In, Out](config: PubSubConfig)(flow: Flow[In, Out, NotUsed]): Flow[In, Out, NotUsed] =
    flow.addAttributes(config.settings.fold(Attributes.none)(GoogleAttributes.settings))

}
