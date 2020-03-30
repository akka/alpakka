/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.impl._
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

object GooglePubSub extends GooglePubSub {
  private[pubsub] override val httpApi = PubSubApi
}

protected[pubsub] trait GooglePubSub {
  private[pubsub] def httpApi: PubSubApi

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids.
   */
  def publish(topic: String,
              config: PubSubConfig,
              parallelism: Int = 1): Flow[PublishRequest, immutable.Seq[String], NotUsed] =
    Flow[PublishRequest]
      .map((_, ()))
      .via(
        publishWithContext[Unit](topic, config, parallelism).asFlow
      )
      .map(_._1)

  /**
   * Creates a flow to that publishes messages to a topic and emits the message ids and carries a context
   * through.
   */
  def publishWithContext[C](
      topic: String,
      config: PubSubConfig,
      parallelism: Int = 1
  ): FlowWithContext[PublishRequest, C, immutable.Seq[String], C, NotUsed] = {
    // some wrapping back and forth as FlowWithContext doesn't offer `setup`
    // https://github.com/akka/akka/issues/27883
    FlowWithContext.fromTuples {
      Flow
        .setup { (mat, _) =>
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          httpApi
            .accessTokenWithContext[PublishRequest, C](config)
            .via(
              httpApi.publish[C](config.projectId, topic, parallelism)
            )
            .asFlow
        }
        .mapMaterializedValue(_ => NotUsed)
    }
  }

  /**
   * Creates a source pulling messages from subscription
   */
  def subscribe(subscription: String, config: PubSubConfig): Source[ReceivedMessage, Cancellable] = {
    Source
      .tick(0.seconds, 1.second, Done)
      .via(subscribeFlow(subscription, config))
  }

  /**
   * Creates a flow pulling messages from subscription
   */
  def subscribeFlow(subscription: String, config: PubSubConfig): Flow[Done, ReceivedMessage, Future[NotUsed]] = {
    Flow
      .setup { (mat, _) =>
        implicit val system: ActorSystem = mat.system
        implicit val materializer: Materializer = mat
        Flow[Done]
          .via(httpApi.accessToken[Done](config))
          .via(
            httpApi
              .pull(config.projectId,
                    subscription,
                    config.pullReturnImmediately,
                    config.pullMaxMessagesPerInternalBatch)
          )
          .mapConcat(_.receivedMessages.getOrElse(Seq.empty[ReceivedMessage]).toIndexedSeq)
      }
  }

  /**
   * Creates a sink for acknowledging messages on subscription
   */
  @deprecated("Use `acknowledge` without `parallelism` param", since = "2.0.0")
  def acknowledge(subscription: String,
                  config: PubSubConfig,
                  parallelism: Int = 1): Sink[AcknowledgeRequest, Future[Done]] =
    acknowledge(subscription, config)

  /**
   * Creates a sink for acknowledging messages on subscription
   */
  def acknowledge(subscription: String, config: PubSubConfig): Sink[AcknowledgeRequest, Future[Done]] = {
    val flow =
      Flow
        .setup { (mat, _) =>
          implicit val system: ActorSystem = mat.system
          implicit val materializer: Materializer = mat
          Flow[AcknowledgeRequest]
            .via(httpApi.accessToken[AcknowledgeRequest](config))
            .via(httpApi.acknowledge(config.projectId, subscription))
        }
    flow
      .toMat(Sink.ignore)(Keep.right)
  }

}
