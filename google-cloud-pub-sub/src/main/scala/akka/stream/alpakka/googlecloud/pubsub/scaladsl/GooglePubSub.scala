/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.impl._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.concurrent.duration._
import scala.collection.immutable
import scala.concurrent.Future

object GooglePubSub extends GooglePubSub {
  private[pubsub] override val httpApi = PubSubApi
}

protected[pubsub] trait GooglePubSub {
  private[pubsub] def httpApi: PubSubApi

  /**
   * Creates a flow to that publish messages to a topic and emits the message ids
   */
  def publish[C](topic: String, config: PubSubConfig, parallelism: Int = 1)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer
  ): Flow[(PublishRequest, C), (immutable.Seq[String], C), NotUsed] =
    httpApi
      .accessTokenWithContext[PublishRequest, C](config)
      .via(
        httpApi.publish[C](config.projectId, topic, parallelism)
      )

  /**
   * Creates a source pulling messages from subscription
   */
  def subscribe(subscription: String, config: PubSubConfig)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer
  ): Source[ReceivedMessage, Cancellable] =
    Source
      .tick(0.seconds, 1.second, Done)
      .via(httpApi.accessToken[Done](config))
      .via(
        httpApi
          .pull(config.projectId, subscription, config.pullReturnImmediately, config.pullMaxMessagesPerInternalBatch)
      )
      .mapConcat(_.receivedMessages.getOrElse(Seq.empty[ReceivedMessage]).toIndexedSeq)

  /**
   * Creates a sink for acknowledging messages on subscription
   */
  def acknowledge(subscription: String, config: PubSubConfig)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer
  ): Sink[AcknowledgeRequest, Future[Done]] =
    Flow[AcknowledgeRequest]
      .via(httpApi.accessToken[AcknowledgeRequest](config))
      .via(httpApi.acknowledge(config.projectId, subscription))
      .toMat(Sink.ignore)(Keep.right)

}
