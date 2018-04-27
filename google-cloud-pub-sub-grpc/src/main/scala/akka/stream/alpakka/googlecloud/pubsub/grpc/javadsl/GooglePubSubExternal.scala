/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.Cancellable
import akka.stream.javadsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1._

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
   * @param publisher used for publishing gRPC messages
   */
  def publish(parallelism: Int, publisher: GrpcPublisher): Flow[PublishRequest, PublishResponse, NotUsed] =
    Flow
      .create[PublishRequest]
      .mapAsyncUnordered(parallelism, javaFunction(publisher.client.publish))

  /**
   * Create a source that emits messages for a given subscription.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   * @param subscriber used for subscribing to subscriptions
   */
  def subscribe(request: StreamingPullRequest,
                pollInterval: Duration,
                subscriber: GrpcSubscriber): Source[ReceivedMessage, CompletableFuture[Cancellable]] = {
    val cancellable = new CompletableFuture[Cancellable]()

    val subsequentRequest = request.toBuilder
      .setSubscription("")
      .setStreamAckDeadlineSeconds(0)
      .build()

    subscriber.client
      .streamingPull(
        Source
          .single(request)
          .concat(
            Source
              .tick(pollInterval, pollInterval, subsequentRequest)
              .mapMaterializedValue(javaFunction(cancellable.complete))
          )
      )
      .mapConcat(javaFunction(_.getReceivedMessagesList))
      .mapMaterializedValue(javaFunction(_ => cancellable))
  }

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   * @param subscriber used for sending acknowledgements
   */
  def acknowledge(parallelism: Int, subscriber: GrpcSubscriber): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    Flow
      .create[AcknowledgeRequest]
      .mapAsyncUnordered(parallelism, javaFunction(subscriber.client.acknowledge))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Helper for creating akka.japi.function.Function instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def javaFunction[A, B](f: A => B): akka.japi.function.Function[A, B] =
    new akka.japi.function.Function[A, B]() {
      override def apply(a: A): B = f(a)
    }
}
