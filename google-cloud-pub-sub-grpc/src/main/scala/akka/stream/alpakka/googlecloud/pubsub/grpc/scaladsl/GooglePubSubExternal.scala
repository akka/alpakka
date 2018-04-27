/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.Cancellable
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1.pubsub._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

/**
 * Google Pub/Sub operator factory.
 *
 * Operators use the provided GrpcPublisher/GrpcSubscriber instances.
 */
object GooglePubSubExternal {

  /**
   * Create a flow to publish messages to Google Cloud Pub/Sub. The flow emits responses that contain published
   * message ids.
   *
   * @param parallelism controls how many messages can be in-flight at any given time
   * @param publisher used for publishing messages
   */
  def publish(parallelism: Int)(implicit publisher: GrpcPublisher): Flow[PublishRequest, PublishResponse, NotUsed] =
    Flow[PublishRequest]
      .mapAsyncUnordered(parallelism)(publisher.client.publish)

  /**
   * Create a source that emits messages for a given subscription.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   * @param subscriber used for subscribing to subscriptions
   */
  def subscribe(
      request: StreamingPullRequest,
      pollInterval: FiniteDuration
  )(implicit subscriber: GrpcSubscriber): Source[ReceivedMessage, Future[Cancellable]] = {
    val cancellable = Promise[Cancellable]

    val subsequentRequest = request
      .withSubscription("")
      .withStreamAckDeadlineSeconds(0)

    subscriber.client
      .streamingPull(
        Source
          .single(request)
          .concat(
            Source
              .tick(0.seconds, pollInterval, ())
              .map(_ => subsequentRequest)
              .mapMaterializedValue(cancellable.success)
          )
      )
      .mapConcat(_.receivedMessages.toVector)
      .mapMaterializedValue(_ => cancellable.future)
  }

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   * @param subscriber used for sending acknowledgements
   */
  def acknowledge(parallelism: Int)(implicit subscriber: GrpcSubscriber): Sink[AcknowledgeRequest, Future[Done]] =
    Flow[AcknowledgeRequest]
      .mapAsyncUnordered(parallelism)(subscriber.client.acknowledge)
      .toMat(Sink.ignore)(Keep.right)
}
