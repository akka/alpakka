/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.{ActorSystem, Cancellable}
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.google.pubsub.v1.pubsub._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Google Pub/Sub operator factory.
 *
 * Operators use the GrpcPublisher/GrpcSubscriber that is resolved from the provided actor system.
 */
object GooglePubSub {

  /**
   * Create a flow to publish messages to Google Cloud Pub/Sub. The flow emits responses that contain published
   * message ids.
   *
   * @param parallelism controls how many messages can be in-flight at any given time
   * @param sys actor system that is used to resolve the GrpcPublisher extension
   */
  def publish(parallelism: Int)(implicit sys: ActorSystem): Flow[PublishRequest, PublishResponse, NotUsed] = {
    val grpc = GrpcPublisherExt()
    import grpc.publisher
    GooglePubSubExternal.publish(parallelism)
  }

  /**
   * Create a source that emits messages for a given subscription.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   * @param sys actor system that is used to resolve the GrpcSubscriber extension
   */
  def subscribe(
      request: StreamingPullRequest,
      pollInterval: FiniteDuration
  )(implicit sys: ActorSystem): Source[ReceivedMessage, Future[Cancellable]] = {
    val grpc = GrpcSubscriberExt()
    import grpc.subscriber
    GooglePubSubExternal.subscribe(request, pollInterval)
  }

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   * @param sys actor system that is used to resolve the GrpcSubscriber extension
   */
  def acknowledge(parallelism: Int)(implicit sys: ActorSystem): Sink[AcknowledgeRequest, Future[Done]] = {
    val grpc = GrpcSubscriberExt()
    import grpc.subscriber
    GooglePubSubExternal.acknowledge(parallelism)
  }
}
