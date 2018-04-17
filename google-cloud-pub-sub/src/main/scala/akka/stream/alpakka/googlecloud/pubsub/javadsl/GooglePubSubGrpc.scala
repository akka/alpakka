/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.{GooglePubSubGrpc => GPubSubGrpc}
import akka.stream.javadsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1

import scala.compat.java8.FutureConverters._

object GooglePubSubGrpc {

  def of(project: String,
         subscription: String,
         parallelism: Int,
         pubSubConfig: PubSubConfig,
         retryOnFailure: Boolean,
         maxConsecutiveFailures: Int,
         actorSystem: ActorSystem,
         materializer: Materializer): GooglePubSubGrpcJava =
    new GooglePubSubGrpcJava(
      project,
      subscription,
      parallelism,
      pubSubConfig,
      retryOnFailure,
      maxConsecutiveFailures,
      actorSystem,
      materializer
    )

  class GooglePubSubGrpcJava(
      project: String,
      subscription: String,
      parallelism: Int,
      pubSubConfig: PubSubConfig,
      retryOnFailure: Boolean,
      maxConsecutiveFailures: Int,
      actorSystem: ActorSystem,
      materializer: Materializer
  ) {
    private val underlying =
      GPubSubGrpc.apply(project, subscription, pubSubConfig, retryOnFailure, maxConsecutiveFailures, parallelism)(
        materializer
      )

    def publish: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed] = underlying.publish.asJava
    def subscribe: Source[v1.ReceivedMessage, NotUsed] = underlying.subscribe(actorSystem).asJava
    def acknowledge: Sink[AcknowledgeRequest, CompletionStage[Done]] =
      underlying.acknowledge.mapMaterializedValue(_.toJava).asJava
  }
}
