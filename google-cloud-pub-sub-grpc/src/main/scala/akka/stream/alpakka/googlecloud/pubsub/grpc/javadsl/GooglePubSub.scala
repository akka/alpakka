/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.Cancellable
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.javadsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1._

/**
 * Google Pub/Sub Akka Stream operator factory.
 */
object GooglePubSub {

  /**
   * Create a flow to publish messages to Google Cloud Pub/Sub. The flow emits responses that contain published
   * message ids.
   *
   * @param parallelism controls how many messages can be in-flight at any given time
   */
  def publish(parallelism: Int): Flow[PublishRequest, PublishResponse, NotUsed] =
    Flow
      .setup { (mat, attr) =>
        Flow
          .create[PublishRequest]
          .mapAsyncUnordered(parallelism, japiFunction(publisher(mat, attr).client.publish))
      }
      .mapMaterializedValue(japiFunction(_ => NotUsed))

  /**
   * Create a source that emits messages for a given subscription using a StreamingPullRequest.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   */
  def subscribe(request: StreamingPullRequest,
                pollInterval: Duration): Source[ReceivedMessage, CompletableFuture[Cancellable]] =
    Source
      .setup { (mat, attr) =>
        val cancellable = new CompletableFuture[Cancellable]()

        val subsequentRequest = request.toBuilder
          .setSubscription("")
          .setStreamAckDeadlineSeconds(0)
          .build()

        subscriber(mat, attr).client
          .streamingPull(
            Source
              .single(request)
              .concat(
                Source
                  .tick(pollInterval, pollInterval, subsequentRequest)
                  .mapMaterializedValue(japiFunction(cancellable.complete))
              )
          )
          .mapConcat(japiFunction(_.getReceivedMessagesList))
          .mapMaterializedValue(japiFunction(_ => cancellable))
      }
      .mapMaterializedValue(japiFunction(flattenCs))
      .mapMaterializedValue(japiFunction(_.toCompletableFuture))

  /**
   * Create a source that emits messages for a given subscription using a synchronous PullRequest.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS field is mandatory for the request
   * @param pollInterval time between PullRequest messages are being sent
   */
  def subscribePolling(
      request: PullRequest,
      pollInterval: Duration
  ): Source[ReceivedMessage, CompletableFuture[Cancellable]] =
    Source
      .setup { (mat, attr) =>
        val cancellable = new CompletableFuture[Cancellable]()

        val client = subscriber(mat, attr).client

        Source
          .tick(pollInterval, pollInterval, request)
          .mapAsync(1, client.pull)
          .mapConcat(japiFunction(_.getReceivedMessagesList))
          .mapMaterializedValue(japiFunction(cancellable.complete))
          .mapMaterializedValue(japiFunction(_ => cancellable))
      }
      .mapMaterializedValue(japiFunction(flattenCs))
      .mapMaterializedValue(japiFunction(_.toCompletableFuture))

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   */
  def acknowledge(parallelism: Int): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    Sink
      .setup { (mat, attr) =>
        Flow
          .create[AcknowledgeRequest]
          .mapAsyncUnordered(parallelism, japiFunction(subscriber(mat, attr).client.acknowledge))
          .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])
      }
      .mapMaterializedValue(japiFunction(flattenCs))

  /**
   * Helper for creating akka.japi.function.Function instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def japiFunction[A, B](f: A => B): akka.japi.function.Function[A, B] =
    new akka.japi.function.Function[A, B]() {
      override def apply(a: A): B = f(a)
    }

  private def flattenCs[T](f: CompletionStage[_ <: CompletionStage[T]]): CompletionStage[T] =
    f.thenCompose(new java.util.function.Function[CompletionStage[T], CompletionStage[T]] {
      override def apply(t: CompletionStage[T]): CompletionStage[T] = t
    })

  private def publisher(mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Publisher]
      .map(_.publisher)
      .getOrElse(GrpcPublisherExt()(mat.system).publisher)

  private def subscriber(mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Subscriber]
      .map(_.subscriber)
      .getOrElse(GrpcSubscriberExt()(mat.system).subscriber)
}
