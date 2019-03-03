/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.Cancellable
import akka.dispatch.ExecutionContexts
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.Setup
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1.pubsub._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

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
    Setup
      .flow { implicit mat => implicit attr =>
        Flow[PublishRequest]
          .mapAsyncUnordered(parallelism)(publisher().client.publish)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a source that emits messages for a given subscription.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   */
  def subscribe(
      request: StreamingPullRequest,
      pollInterval: FiniteDuration
  ): Source[ReceivedMessage, Future[Cancellable]] =
    Setup
      .source { implicit mat => implicit attr =>
        val cancellable = Promise[Cancellable]

        val subsequentRequest = request
          .withSubscription("")
          .withStreamAckDeadlineSeconds(0)

        subscriber().client
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
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.sameThreadExecutionContext))

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   */
  def acknowledge(parallelism: Int): Sink[AcknowledgeRequest, Future[Done]] =
    Setup
      .sink { implicit mat => implicit attr =>
        Flow[AcknowledgeRequest]
          .mapAsyncUnordered(parallelism)(subscriber().client.acknowledge)
          .toMat(Sink.ignore)(Keep.right)
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.sameThreadExecutionContext))

  private def publisher()(implicit mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Publisher]
      .map(_.publisher)
      .getOrElse(GrpcPublisherExt()(mat.system).publisher)

  private def subscriber()(implicit mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Subscriber]
      .map(_.subscriber)
      .getOrElse(GrpcSubscriberExt()(mat.system).subscriber)
}
